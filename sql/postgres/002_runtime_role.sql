-- The pipeline's runtime login: it can read and append catalog rows and nothing else.
-- No UPDATE, DELETE or TRUNCATE, no DDL, no role creation, no RLS bypass. A compromised
-- pipeline host can add rows but cannot rewrite or erase what is already recorded.
--
-- Usage: psql -v runtime_password='...' -f 002_runtime_role.sql
SELECT 'CREATE ROLE lakehouse_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lakehouse_writer') \gexec

ALTER ROLE lakehouse_writer WITH PASSWORD :'runtime_password';

REVOKE ALL ON SCHEMA lakehouse FROM lakehouse_writer;
REVOKE ALL ON ALL TABLES IN SCHEMA lakehouse FROM lakehouse_writer;
GRANT USAGE ON SCHEMA lakehouse TO lakehouse_writer;
GRANT SELECT, INSERT ON lakehouse.objects TO lakehouse_writer;
