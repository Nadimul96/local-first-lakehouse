-- Object catalog for local-first-lakehouse. Run as the schema owner (the migration role).
CREATE SCHEMA IF NOT EXISTS lakehouse;

CREATE TABLE IF NOT EXISTS lakehouse.objects (
    object_key        TEXT PRIMARY KEY,
    receipt_id        TEXT NOT NULL UNIQUE,
    ciphertext_sha256 TEXT NOT NULL CHECK (ciphertext_sha256 ~ '^[0-9a-f]{64}$'),
    plaintext_sha256  TEXT NOT NULL CHECK (plaintext_sha256 ~ '^[0-9a-f]{64}$'),
    key_id            TEXT NOT NULL,
    encrypted_bytes   BIGINT NOT NULL CHECK (encrypted_bytes > 0),
    receipt           JSONB NOT NULL,
    verified_at       TIMESTAMPTZ NOT NULL
);
