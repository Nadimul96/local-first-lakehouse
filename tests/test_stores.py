from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import pytest

from lakehouse.errors import VerificationError
from lakehouse.stores import check_key
from lakehouse.stores.filesystem import FilesystemObjectStore
from lakehouse.stores.s3 import S3Config, S3ObjectStore


def test_filesystem_store_never_overwrites(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path)
    first = store.put_if_absent("a/b.enc", b"one", metadata={"m": "1"})
    second = store.put_if_absent("a/b.enc", b"two", metadata={"m": "1"})
    assert first.created and not second.created
    assert store.get("a/b.enc", max_bytes=10) == b"one"
    assert store.head("a/b.enc").metadata == {"m": "1"}  # type: ignore[union-attr]
    assert store.head("missing.enc") is None


def test_filesystem_store_enforces_read_cap_and_safe_keys(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path)
    store.put_if_absent("k.enc", b"x" * 10, metadata={})
    with pytest.raises(VerificationError, match="larger"):
        store.get("k.enc", max_bytes=5)
    with pytest.raises(VerificationError, match="does not exist"):
        store.get("nope.enc", max_bytes=5)
    for bad in ("../escape", "/abs", "a/../b", ".hidden"):
        with pytest.raises(ValueError):
            check_key(bad)


class ClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    """Implements the slice of the S3 API the adapter uses, including If-None-Match."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["IfNoneMatch"] == "*"
        if kwargs["Key"] in self.objects:
            raise ClientError("PreconditionFailed")
        self.objects[kwargs["Key"]] = (kwargs["Body"], kwargs["Metadata"])
        return {"ETag": '"etag-1"'}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        if kwargs["Key"] not in self.objects:
            raise ClientError("NoSuchKey")
        return {"Body": io.BytesIO(self.objects[kwargs["Key"]][0])}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        if kwargs["Key"] not in self.objects:
            raise ClientError("404")
        data, meta = self.objects[kwargs["Key"]]
        return {"ContentLength": len(data), "ETag": '"etag-1"', "Metadata": meta}


def test_s3_store_conditional_put_and_prefix() -> None:
    fake = FakeS3()
    store = S3ObjectStore(S3Config(bucket="lake", prefix="tenant-a"), client=fake)
    assert store.put_if_absent("x.enc", b"abc", metadata={"k": "v"}).created
    repeat = store.put_if_absent("x.enc", b"zzz", metadata={"k": "v"})
    assert not repeat.created and repeat.stored.size == 3
    assert "tenant-a/x.enc" in fake.objects
    assert store.get("x.enc", max_bytes=3) == b"abc"
    assert store.head("absent.enc") is None
    with pytest.raises(VerificationError, match="larger"):
        store.get("x.enc", max_bytes=2)
    with pytest.raises(VerificationError, match="does not exist"):
        store.get("absent.enc", max_bytes=2)


def test_s3_config_from_env_requires_a_bucket() -> None:
    from lakehouse.errors import ConfigError

    with pytest.raises(ConfigError):
        S3Config.from_env({})
    config = S3Config.from_env(
        {
            "LAKEHOUSE_S3_BUCKET": "b",
            "LAKEHOUSE_S3_ENDPOINT": "http://localhost:9000",
            "LAKEHOUSE_S3_PATH_STYLE": "true",
        }
    )
    assert config.force_path_style and config.endpoint_url == "http://localhost:9000"
    assert "secret_access_key" not in repr(config)  # credentials never appear in logs
