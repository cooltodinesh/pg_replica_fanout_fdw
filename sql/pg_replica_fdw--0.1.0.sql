/* pg_replica_fdw/sql/pg_replica_fdw--0.1.0.sql */

\echo Use "CREATE EXTENSION pg_replica_fdw" to load this file. \quit

CREATE FUNCTION pg_replica_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pg_replica_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pg_replica_fdw
  HANDLER pg_replica_fdw_handler
  VALIDATOR pg_replica_fdw_validator;
