-- basic.sql: extension/server/mapping/table setup, smoke SELECT, validator checks
\i test/loopback-setup.sql

CREATE TABLE small_t (id int, name text);
INSERT INTO small_t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

CREATE FOREIGN TABLE small_ft (id int, name text)
  SERVER loopback OPTIONS (table_name 'small_t');

SELECT * FROM small_ft ORDER BY id;

SELECT * FROM small_ft
EXCEPT
SELECT * FROM small_t;

-- two concurrently-live foreign scans on the same server (self-join) must
-- raise a clear error, not a bare libpq "another command is already in
-- progress" message.
SELECT count(*) FROM small_ft a JOIN small_ft b USING (id);

-- a single scan node, rescanned many times (correlated subquery), remains
-- legal -- see slicing.sql for the full rescan test.

-- validator: missing required "replicas" option
CREATE SERVER v_missing_replicas FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (dbname 'postgres');

-- validator: consistency 'lsn' rejected (only 'none' is supported)
CREATE SERVER v_bad_consistency FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (replicas 'localhost', consistency 'lsn');

-- validator: unknown option name
CREATE SERVER v_bad_option FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (replicas 'localhost', bogus 'x');
