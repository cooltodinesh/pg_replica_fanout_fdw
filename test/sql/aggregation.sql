-- aggregation.sql (v2 + M3): two aggregation paths coexist.
--
--  * A bare count(*) (optionally with a shippable WHERE) is PUSHED DOWN: a
--    scanrelid==0 Foreign Scan fans "SELECT count(*)" to every replica and
--    combines the partials -- one row per replica on the wire, no coordinator
--    Aggregate node (M3).
--  * Every other aggregate (sum/min/max/avg, multiple aggregates, GROUP BY,
--    count(DISTINCT), ...) uses a native Aggregate over the Append of per-replica
--    Async Foreign Scans: correct, but the raw rows come back and core
--    aggregates them.
--
-- Checks are discriminating: a broken slice map makes the counts wrong, and a
-- stale/1-slice plan shows fewer than 3 Async Foreign Scan children (native
-- path) or the wrong Remote SQL (pushed path).
\i test/loopback-setup.sql

-- Multi-block table so the 3 slices are non-trivial (nblocks >> 3).
CREATE TABLE agg_t (id int, grp int, pad text);
INSERT INTO agg_t SELECT g, g % 4, repeat('q', 900) FROM generate_series(1, 400) g;

DROP SERVER IF EXISTS agg_srv CASCADE;
CREATE SERVER agg_srv FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (replicas 'localhost:5432,localhost:5432,localhost:5432',
           consistency 'none');
CREATE USER MAPPING FOR CURRENT_USER SERVER agg_srv;
CREATE FOREIGN TABLE aft (id int, grp int, pad text)
  SERVER agg_srv OPTIONS (table_name 'agg_t', min_blocks_per_slice '1');

-- 1. Whole-table aggregates: correct answers.
SELECT count(*), sum(id), min(id), max(id), avg(id)::numeric(10,4) FROM aft;

-- 2. Bare count(*) is PUSHED DOWN (M3): a Foreign Scan whose remote SQL is
-- "SELECT count(*)", with no Aggregate node above it.
EXPLAIN (COSTS OFF) SELECT count(*) FROM aft;
SELECT count(*) FROM aft;

-- 3. The native path (any non-count aggregate): a native Aggregate over the
-- Append of 3 Async Foreign Scans.  Under ANALYZE the child "actual rows" sum
-- to the full row count, proving the 3 slices are disjoint and complete.
-- (BUFFERS OFF: PG18+ ANALYZE buffers accounting is noisy.)
EXPLAIN (COSTS OFF) SELECT sum(id) FROM aft;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT sum(id) FROM aft;
SELECT sum(id) FROM aft;

-- 4. GROUP BY -- native, works in v2 (impossible to push in v1).
SELECT grp, count(*), sum(id) FROM aft GROUP BY grp ORDER BY grp;

-- 5. count(*) with a shippable WHERE: still pushed down, with the qual folded
-- into each replica's remote count(*).
EXPLAIN (COSTS OFF) SELECT count(*) FROM aft WHERE id > 100;
SELECT count(*), min(id), max(id) FROM aft WHERE id > 100;

-- 6. count(DISTINCT) is NOT the pushable shape -> native path.
SELECT count(DISTINCT id) FROM aft;

-- 7. Reuse across plan shapes in one session: pushed count, then a plain scan.
SELECT count(*) FROM aft;
SELECT id FROM aft ORDER BY id LIMIT 3;

-- 8. Two concurrent scans of the same server (self-join) are rejected in this
-- iteration -- one connection per replica, so the second scan collides.
SELECT count(*) FROM aft a JOIN aft b USING (id);
