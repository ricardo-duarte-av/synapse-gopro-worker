-- Read-only PostgreSQL role for the GoPro worker.
--
-- The worker only ever reads. This role makes that a guarantee enforced by the
-- database rather than a property of the code: default_transaction_read_only
-- means even a bug that issues a write cannot commit one.
--
-- Run as a superuser against the Synapse database:
--   psql -h /var/sockets -U synapse -d synapse-db -f readonly-role.sql
--
-- To undo:  DROP OWNED BY gopro_ro; DROP ROLE gopro_ro;

BEGIN;

CREATE ROLE gopro_ro WITH LOGIN;

GRANT CONNECT ON DATABASE "synapse-db" TO gopro_ro;
GRANT USAGE ON SCHEMA public TO gopro_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO gopro_ro;

-- Covers tables added by future Synapse schema migrations.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO gopro_ro;

-- Belt and braces: reject writes at the transaction level.
ALTER ROLE gopro_ro SET default_transaction_read_only = on;

-- A query that outlives its request is pure waste.
ALTER ROLE gopro_ro SET statement_timeout = '60s';

COMMIT;

-- Verify: the first succeeds, the second must fail.
--   psql -h /var/sockets -U gopro_ro -d synapse-db -c 'select count(*) from rooms;'
--   psql -h /var/sockets -U gopro_ro -d synapse-db -c 'create table t(x int);'
