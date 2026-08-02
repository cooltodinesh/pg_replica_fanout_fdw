-- exec_modes.sql: the execution mode for a scan is chosen from what the local
-- planner would do with the co-located copy.  A selective qual an index serves
-- is answered locally (no fan-out, no remote); a full scan or a non-selective
-- qual fans out across the replicas by ctid slice.
--
-- Correctness is identical either way (serve-local and fan-out are two ways to
-- compute the same rows); these checks are about *which* plan is chosen and
-- that the chosen plan still returns the right rows.
\i test/loopback-setup.sql

-- A multi-block table with a btree index on id.  id is unique and highly
-- selective (equality picks one row of 5000 -> index scan); val is unindexed.
CREATE TABLE mode_t (id int, val int, pad text);
INSERT INTO mode_t
  SELECT g, g % 100, repeat('x', 60) FROM generate_series(1, 5000) g;
CREATE INDEX mode_t_id_idx ON mode_t (id);
ANALYZE mode_t;

DROP SERVER IF EXISTS mode_srv CASCADE;
CREATE SERVER mode_srv FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (replicas 'localhost:5432,localhost:5432,localhost:5432',
           consistency 'none');
CREATE USER MAPPING FOR CURRENT_USER SERVER mode_srv;
CREATE FOREIGN TABLE mode_ft (id int, val int, pad text)
  SERVER mode_srv OPTIONS (table_name 'mode_t', min_blocks_per_slice '1');

-- 1. A selective qual on the indexed column: the local planner uses the index,
-- so the whole scan is served locally -- a single Foreign Scan with
-- "Fanout Mode: serve-local" and a "Local SQL" line, no Append, no per-replica
-- children.
EXPLAIN (COSTS OFF) SELECT id, val FROM mode_ft WHERE id = 42;
-- ...and it returns the right row.
SELECT id, val FROM mode_ft WHERE id = 42;

-- 2. A selective range on the indexed column is also served locally.
EXPLAIN (COSTS OFF) SELECT id, val FROM mode_ft WHERE id BETWEEN 10 AND 13;
SELECT id, val FROM mode_ft WHERE id BETWEEN 10 AND 13 ORDER BY id;

-- 3. A full scan (no shippable qual) has nothing for an index to serve, so it
-- fans out: an Append of per-replica Async Foreign Scans, no mode line.
EXPLAIN (COSTS OFF) SELECT id FROM mode_ft ORDER BY id LIMIT 3;

-- 4. A non-selective qual (val >= 0 matches every row, and val is unindexed):
-- the local planner picks a seq scan, so a row scan fans out too -- the probe
-- correctly declines serve-local when an index wouldn't help.  (The qual is
-- still pushed into each per-replica template.)
EXPLAIN (COSTS OFF) SELECT id FROM mode_ft WHERE val >= 0;
SELECT count(*) FROM mode_ft WHERE val >= 0;

-- 5. Rescan: a serve-local scan as the inner of a nested loop is rewound once
-- per outer row and must return its rows every time (cursor reset, no
-- re-execution).  Three outer rows x the one matching inner row = 3.
SELECT count(*)
  FROM generate_series(1, 3) g,
       LATERAL (SELECT id FROM mode_ft WHERE id = 42) s;

-- 6. Serve-local result equals fan-out result for the same predicate: force a
-- fan-out of the same rows via a non-shippable duplicate of the qual and
-- compare.  (id = 42 is shippable -> serve-local; id = 42 AND id + 0 = 42 keeps
-- the shippable id = 42, so still serve-local -- so instead compare against the
-- local table directly.)
SELECT (SELECT id FROM mode_ft WHERE id = 42)
     = (SELECT id FROM mode_t  WHERE id = 42) AS serve_local_matches_local;

-- 7. A large IN-list on the indexed column is index-served AND has many distinct
-- values, so the value list is split across the replicas: an Append of P
-- children, each an Async Foreign Scan whose Remote SQL carries a disjoint,
-- key-sorted chunk of the values and NO ctid bound.
EXPLAIN (COSTS OFF)
  SELECT id FROM mode_ft WHERE id IN (5,10,15,20,25,30,35,40,45,50,55,60);
-- exactly those rows, once each
SELECT id FROM mode_ft WHERE id IN (5,10,15,20,25,30,35,40,45,50,55,60)
  ORDER BY id;
-- the split's union count equals the un-split local count
SELECT (SELECT count(*) FROM mode_ft
          WHERE id IN (5,10,15,20,25,30,35,40,45,50,55,60))
     = (SELECT count(*) FROM mode_t
          WHERE id IN (5,10,15,20,25,30,35,40,45,50,55,60)) AS in_split_count_matches;

-- 8. A small IN-list (below the split threshold) on the indexed column is served
-- locally, not split.
EXPLAIN (COSTS OFF) SELECT id FROM mode_ft WHERE id IN (5, 10, 15);
SELECT id FROM mode_ft WHERE id IN (5, 10, 15) ORDER BY id;

-- 9. Duplicate values in a splittable IN-list must not yield duplicate rows:
-- the values are de-duplicated before splitting, so each distinct value lands
-- in exactly one chunk.  Ten distinct values (>= threshold), two duplicated.
SELECT id FROM mode_ft
  WHERE id IN (5,5,10,10,15,20,25,30,35,40,45,50) ORDER BY id;
