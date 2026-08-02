-- errors.sql: a server whose replicas include an unreachable host errors
-- cleanly, names the offending replica, and respects connect_timeout
-- instead of hanging.
\i test/loopback-setup.sql

-- Assumes 10.255.255.1:5999 is blackholed (times out) rather than actively
-- refused on the CI/dev host; a host that rejects it immediately would
-- produce a different message/timing and diff against the expected output.
DROP SERVER IF EXISTS bad_loopback CASCADE;
CREATE SERVER bad_loopback FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (replicas 'localhost:5432,10.255.255.1:5999', connect_timeout '2');
CREATE USER MAPPING FOR CURRENT_USER SERVER bad_loopback;

CREATE TABLE err_t (id int);
INSERT INTO err_t SELECT generate_series(1, 5);

CREATE FOREIGN TABLE err_ft (id int)
  SERVER bad_loopback OPTIONS (table_name 'err_t');

SELECT * FROM err_ft;

-- A missing table is caught at the coordinator: the co-located local copy
-- drives the ctid slice bounds, so its absence fails fast with a clear local
-- error before any remote work.  The session must stay healthy afterwards --
-- the next query still succeeds, no reconnect.
CREATE FOREIGN TABLE missing_ft (id int)
  SERVER loopback OPTIONS (table_name 'does_not_exist');

SELECT * FROM missing_ft;
SELECT count(*) FROM small_ft;
