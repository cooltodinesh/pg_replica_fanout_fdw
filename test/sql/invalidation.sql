-- invalidation.sql: ALTER SERVER / ALTER USER MAPPING must take effect on the
-- next query in the same session (the cached ReplicaSet is rebuilt), not
-- require a reconnect.
--
-- Both checks below are deliberately *discriminating*: they fail if the
-- syscache-invalidation path regresses.
--   * ALTER SERVER: EXPLAIN ANALYZE actually executes the scan, so the node's
--     "Replicas:" line reports nslices, which is capped by the CACHED nconns.
--     A stale cache would keep reporting the old replica count.  (Plain EXPLAIN
--     is NOT used here: it short-circuits on EXEC_FLAG_EXPLAIN_ONLY and prints
--     the freshly-parsed option list, which changes even if the cache doesn't.)
--   * ALTER USER MAPPING: switch the mapping to a role without SELECT on the
--     base table.  A correctly-invalidated cache reconnects as that role and
--     the query fails "permission denied"; a stale cache keeps using the
--     original privileged connection and would wrongly succeed.
\i test/loopback-setup.sql

-- Multi-block table so that, with min_blocks_per_slice=1, nslices tracks nconns
-- (P = Min(nconns, nblocks) and nblocks >> 3 here).
CREATE TABLE inv_t (id int, pad text);
INSERT INTO inv_t SELECT g, repeat('q', 900) FROM generate_series(1, 400) g;

DROP SERVER IF EXISTS inv_srv CASCADE;
CREATE SERVER inv_srv FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (replicas 'localhost:5432', consistency 'none');
CREATE USER MAPPING FOR CURRENT_USER SERVER inv_srv;
CREATE FOREIGN TABLE inv_ft (id int, pad text)
  SERVER inv_srv OPTIONS (table_name 'inv_t', min_blocks_per_slice '1');

-- Cache the ReplicaSet at nconns=1, and observe it via EXPLAIN ANALYZE.
-- (BUFFERS OFF: ANALYZE enables buffer accounting by default on PG18+, and the
-- "Planning: Buffers:" line is catalog-cache-warmth dependent -- i.e. flaky.)
SELECT count(*) FROM inv_ft;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT * FROM inv_ft;

-- Widen the replica list: the cached ReplicaSet must be rebuilt.  If it is,
-- nslices (and thus "Replicas:") goes 1 -> 3; a stale cache would stay at 1.
ALTER SERVER inv_srv OPTIONS (SET replicas 'localhost:5432,localhost:5432,localhost:5432');
SELECT count(*) FROM inv_ft;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT * FROM inv_ft;

-- ALTER USER MAPPING must also rebuild the connections.  Point the mapping at
-- a role that cannot read the base table.
DROP ROLE IF EXISTS inv_lowpriv;
CREATE ROLE inv_lowpriv LOGIN NOSUPERUSER;

ALTER USER MAPPING FOR CURRENT_USER SERVER inv_srv OPTIONS (ADD user 'inv_lowpriv');
-- Correct invalidation => reconnect as inv_lowpriv => permission denied.
-- Stale cache => still connected as the privileged user => wrongly returns 400.
SELECT count(*) FROM inv_ft;

-- Restore an access-capable mapping and confirm the session recovers.
ALTER USER MAPPING FOR CURRENT_USER SERVER inv_srv OPTIONS (DROP user);
SELECT count(*) FROM inv_ft;

DROP ROLE inv_lowpriv;
