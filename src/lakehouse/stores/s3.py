"""S3-compatible object store (AWS S3, MinIO, Cloudflare R2) via boto3.

`put_if_absent` uses a conditional PUT (`If-None-Match: *`), so a retried upload can
never overwrite an object: the provider answers 412 and we report `created=False`. The
credential only needs PutObject, GetObject and HeadObject on the prefix; no delete.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ..errors import ConfigError, DependencyMissingError, VerificationError
from . import PutResult, StoredObject, check_key

_MISSING = {"404", "NoSuchKey", "NotFound"}
_PRECONDITION = {"412", "PreconditionFailed", "ConditionalRequestConflict"}


@dataclass(frozen=True)
class S3Config:
    bucket: str
    endpoint_url: str | None = None
    region: str = "us-east-1"
    prefix: str = "lakehouse"
    force_path_style: bool = False
    access_key_id: str | None = field(default=None, repr=False)
    secret_access_key: str | None = field(default=None, repr=False)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> S3Config:
        env = os.environ if environ is None else environ
        bucket = env.get("LAKEHOUSE_S3_BUCKET", "")
        if not bucket:
            raise ConfigError("LAKEHOUSE_STORE=s3 needs LAKEHOUSE_S3_BUCKET")
        return cls(
            bucket=bucket,
            endpoint_url=env.get("LAKEHOUSE_S3_ENDPOINT") or None,
            region=env.get("LAKEHOUSE_S3_REGION", "us-east-1"),
            prefix=env.get("LAKEHOUSE_S3_PREFIX", "lakehouse").strip("/"),
            force_path_style=env.get("LAKEHOUSE_S3_PATH_STYLE", "").lower() in {"1", "true"},
            # Unset means boto3's default chain (env vars, profile, instance role).
            access_key_id=env.get("AWS_ACCESS_KEY_ID") or None,
            secret_access_key=env.get("AWS_SECRET_ACCESS_KEY") or None,
        )


def _error_code(error: Exception) -> str:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        return str(response.get("Error", {}).get("Code", ""))
    return ""


class S3ObjectStore:
    provider = "s3"

    def __init__(self, config: S3Config, *, client: Any | None = None) -> None:
        self.config = config
        self.bucket = config.bucket
        self._client = client if client is not None else _make_client(config)

    def _full_key(self, key: str) -> str:
        check_key(key)
        return f"{self.config.prefix}/{key}" if self.config.prefix else key

    def put_if_absent(self, key: str, data: bytes, *, metadata: Mapping[str, str]) -> PutResult:
        full_key = self._full_key(key)
        try:
            response = self._client.put_object(
                Bucket=self.bucket,
                Key=full_key,
                Body=data,
                ContentLength=len(data),
                ContentType="application/octet-stream",
                Metadata=dict(metadata),
                IfNoneMatch="*",
            )
        except Exception as error:
            if _error_code(error) not in _PRECONDITION:
                raise
            existing = self.head(key)
            if existing is None:
                raise VerificationError(f"conditional put on {key} failed but no object") from error
            return PutResult(existing, created=False)
        etag = str(response.get("ETag", "")).strip('"') or None
        return PutResult(StoredObject(key, len(data), etag, dict(metadata)), created=True)

    def get(self, key: str, *, max_bytes: int) -> bytes:
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=self._full_key(key))
        except Exception as error:
            if _error_code(error) in _MISSING:
                raise VerificationError(f"object {key} does not exist") from error
            raise
        body = response["Body"]
        chunks: list[bytes] = []
        size = 0
        try:
            while chunk := body.read(1024 * 1024):
                size += len(chunk)
                if size > max_bytes:
                    raise VerificationError(f"object {key} is larger than expected")
                chunks.append(chunk)
        finally:
            body.close()
        return b"".join(chunks)

    def head(self, key: str) -> StoredObject | None:
        try:
            response = self._client.head_object(Bucket=self.bucket, Key=self._full_key(key))
        except Exception as error:
            if _error_code(error) in _MISSING:
                return None
            raise
        return StoredObject(
            key=key,
            size=int(response.get("ContentLength", 0)),
            etag=str(response.get("ETag", "")).strip('"') or None,
            metadata={str(k).lower(): str(v) for k, v in response.get("Metadata", {}).items()},
        )


def _make_client(config: S3Config) -> Any:
    try:
        boto3 = importlib.import_module("boto3")
        botocore_config = importlib.import_module("botocore.config")
    except ImportError as error:
        raise DependencyMissingError("install the s3 extra: uv sync --extra s3") from error
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        region_name=config.region,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        config=botocore_config.Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if config.force_path_style else "virtual"},
            retries={"mode": "standard", "max_attempts": 4},
        ),
    )
