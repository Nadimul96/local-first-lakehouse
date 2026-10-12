# local-first-lakehouse

A local-first, client-side-encrypted medallion pipeline: a durable local event log feeds
immutable Bronze Parquet, which is sealed with AES-256-GCM, uploaded through a leased queue
with read-back verification, and modeled into Silver and Gold with DuckDB.

Work in progress.
