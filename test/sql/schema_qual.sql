-- schema_qual.sql: the remote table a foreign table stands for is identified by
-- (schema_name, table_name).  Both default to the foreign table's OWN namespace
-- and name and can be overridden per option (option.c).  The resolved schema is
-- used in two independent places:
--   * the deparsed template sent to replicas: quote_qualified_identifier(schema,
--     table) -> "FROM <schema>.<table>" (deparse.c), and
--   * the local block-count probe RepFdwGetNBlocks(schema, table), which does
--     get_namespace_oid(schema) to size the ctid slices (slice.c).
-- Every other suite puts its real tables in public; here we exercise non-public
-- schemas, an explicit schema_name option, and identifiers that require quoting,
-- checking both the deparse (Remote SQL Template) and that scans return the
-- right rows (a schema mis-resolution in either place would error or drop rows).
\i test/loopback-setup.sql

-- 1. Default schema resolution: the foreign table itself lives in a non-public
-- schema and carries no schema_name option, so schema_name defaults to the
-- foreign table's own namespace (sq_app).  Remote SQL Template must read
-- "FROM sq_app.data_t", and a full-scan (ctid-slice) fan-out must return the
-- rows -- which independently exercises RepFdwGetNBlocks(sq_app, data_t).
CREATE SCHEMA sq_app;
CREATE TABLE sq_app.data_t (id int, val int);
INSERT INTO sq_app.data_t SELECT g, g % 10 FROM generate_series(1, 500) g;
ANALYZE sq_app.data_t;

CREATE FOREIGN TABLE sq_app.data_ft (id int, val int)
  SERVER loopback OPTIONS (table_name 'data_t', min_blocks_per_slice '1');

EXPLAIN (COSTS OFF) SELECT id FROM sq_app.data_ft ORDER BY id LIMIT 3;
SELECT count(*), sum(id) FROM sq_app.data_ft;
-- full scan through the ctid-slice path equals the local table, exactly
SELECT * FROM sq_app.data_ft EXCEPT SELECT * FROM sq_app.data_t;
SELECT * FROM sq_app.data_t  EXCEPT SELECT * FROM sq_app.data_ft;

-- 2. Explicit schema_name option: the foreign table lives in public but points
-- at a table in another schema via schema_name.  The option value wins over the
-- foreign table's own namespace; Remote SQL Template must read
-- "FROM sq_other.thing_t".
CREATE SCHEMA sq_other;
CREATE TABLE sq_other.thing_t (id int, val int);
INSERT INTO sq_other.thing_t SELECT g, g % 7 FROM generate_series(1, 500) g;
ANALYZE sq_other.thing_t;

CREATE FOREIGN TABLE sq_thing_ft (id int, val int)
  SERVER loopback
  OPTIONS (schema_name 'sq_other', table_name 'thing_t', min_blocks_per_slice '1');

EXPLAIN (COSTS OFF) SELECT id FROM sq_thing_ft ORDER BY id LIMIT 3;
SELECT count(*), sum(id) FROM sq_thing_ft;
SELECT * FROM sq_thing_ft   EXCEPT SELECT * FROM sq_other.thing_t;
SELECT * FROM sq_other.thing_t EXCEPT SELECT * FROM sq_thing_ft;

-- 3. Identifiers that require quoting: both the schema and the table are
-- mixed-case, so the option values are the raw (unquoted) names but the deparse
-- must quote them -- Remote SQL Template must read FROM "SqMixed"."MixTbl".
-- RepFdwGetNBlocks resolves the same raw name via get_namespace_oid, so the
-- ctid-slice fan-out (the EXCEPT below) proves both paths quote-resolve alike.
CREATE SCHEMA "SqMixed";
CREATE TABLE "SqMixed"."MixTbl" (id int, val int);
INSERT INTO "SqMixed"."MixTbl" SELECT g, g % 3 FROM generate_series(1, 500) g;
ANALYZE "SqMixed"."MixTbl";

CREATE FOREIGN TABLE sq_mixed_ft (id int, val int)
  SERVER loopback
  OPTIONS (schema_name 'SqMixed', table_name 'MixTbl', min_blocks_per_slice '1');

EXPLAIN (COSTS OFF) SELECT id FROM sq_mixed_ft ORDER BY id LIMIT 3;
SELECT count(*), sum(id) FROM sq_mixed_ft;
SELECT * FROM sq_mixed_ft      EXCEPT SELECT * FROM "SqMixed"."MixTbl";
SELECT * FROM "SqMixed"."MixTbl" EXCEPT SELECT * FROM sq_mixed_ft;

-- cleanup so re-runs against the same regression DB are idempotent
DROP FOREIGN TABLE sq_app.data_ft, sq_thing_ft, sq_mixed_ft;
DROP SCHEMA sq_app CASCADE;
DROP SCHEMA sq_other CASCADE;
DROP SCHEMA "SqMixed" CASCADE;
