-- types_nulls.sql: text-protocol materialization round-trips exactly,
-- including interspersed NULLs, across the supported scalar types.
\i test/loopback-setup.sql

CREATE TABLE types_t (
  id          int4,
  big         int8,
  t           text,
  num         numeric,
  flag        bool,
  ts          timestamptz
);

INSERT INTO types_t VALUES
  (1, 10000000000, 'hello',   3.14159,  true,  '2024-01-01 00:00:00+00'),
  (2, NULL,         NULL,     NULL,     NULL,  NULL),
  (3, -5,           'world',  -2.5,     false, '2030-06-15 12:34:56+00'),
  (4, 0,            '',       0,        true,  'epoch'),
  (5, NULL,         'mixed',  NULL,     false, '1999-12-31 23:59:59+00'),
  (NULL, 99,        'nullid', 1.5,      true,  '2024-01-01 00:00:00+00');

CREATE FOREIGN TABLE types_ft (
  id          int4,
  big         int8,
  t           text,
  num         numeric,
  flag        bool,
  ts          timestamptz
) SERVER loopback OPTIONS (table_name 'types_t', min_blocks_per_slice '1');

SELECT count(*) FROM (SELECT * FROM types_ft EXCEPT SELECT * FROM types_t) x;
SELECT count(*) FROM (SELECT * FROM types_t EXCEPT SELECT * FROM types_ft) x;

SELECT * FROM types_ft ORDER BY id NULLS LAST, big NULLS LAST;

-- column_name remap: the foreign table's local column name differs from
-- the remote one, and the deparsed SELECT must use the remote name.
CREATE FOREIGN TABLE types_ft2 (
  id          int4,
  body        text OPTIONS (column_name 't')
) SERVER loopback OPTIONS (table_name 'types_t', min_blocks_per_slice '1');

SELECT count(*) FROM (
  SELECT id, body FROM types_ft2 EXCEPT SELECT id, t FROM types_t
) x;
SELECT count(*) FROM (
  SELECT id, t FROM types_t EXCEPT SELECT id, body FROM types_ft2
) x;
