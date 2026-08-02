# pg_replica_fanout_fdw/Makefile

MODULE_big = pg_replica_fanout_fdw
OBJS = src/pg_replica_fanout_fdw.o src/option.o src/connection.o src/deparse.o \
       src/slice.o

EXTENSION = pg_replica_fanout_fdw
DATA = sql/pg_replica_fanout_fdw--0.1.0.sql

REGRESS = basic slicing types_nulls errors invalidation aggregation qual_pushdown \
          exec_modes
REGRESS_OPTS = --inputdir=test --outputdir=test

PG_CONFIG = pg_config
PG_CPPFLAGS = -I$(srcdir)/src -I$(shell $(PG_CONFIG) --includedir)
SHLIB_LINK = -L$(shell $(PG_CONFIG) --libdir) -lpq

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
