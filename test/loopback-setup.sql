-- test/loopback-setup.sql
--
-- Shared setup for the loopback regress suites: point all three "replicas"
-- at the same local instance.  Disjoint ctid slices still union to exactly
-- the whole table, so slicing/fan-out/merge are fully exercised on one
-- instance.  Idempotent, safe to \i at the top of any suite.
-- Idempotent by design (IF NOT EXISTS throughout, no DROP/CASCADE): this
-- file is \i'd at the top of every suite, and suites run sequentially
-- against the same regression database.
CREATE EXTENSION IF NOT EXISTS pg_replica_fdw;

CREATE SERVER IF NOT EXISTS loopback FOREIGN DATA WRAPPER pg_replica_fdw
  OPTIONS (replicas 'localhost:5432,localhost:5432,localhost:5432',
           consistency 'none');

CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER loopback;
