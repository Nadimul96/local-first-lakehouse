#!/bin/sh
# Runs once when the Postgres container initializes an empty data directory.
set -eu
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /sql/001_catalog.sql
psql -v ON_ERROR_STOP=1 -v runtime_password="$LAKEHOUSE_PG_RUNTIME_PASSWORD" \
  -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /sql/002_runtime_role.sql
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "GRANT CONNECT ON DATABASE \"$POSTGRES_DB\" TO lakehouse_writer"
