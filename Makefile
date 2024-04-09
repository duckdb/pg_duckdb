.PHONY: duckdb install_duckdb

MODULE_big = quack
EXTENSION = quack
DATA = quack.control $(wildcard quack--*.sql)

SRCS = src/quack_heap_scan.cpp \
			src/quack_hooks.cpp \
			src/quack_select.cpp \
			src/quack_types.cpp \
			src/quack.cpp

OBJS = $(subst .cpp,.o, $(SRCS))

PG_CONFIG ?= pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_LIB := $(shell $(PG_CONFIG) --pkglibdir)
PG_CPPFLAGS := -Iinclude -Ithird_party/duckdb/src/include -std=c++17
SHLIB_LINK += -Wl,-rpath,$(PG_LIB)/ -L$(PG_LIB) -lduckdb -Lthird_party/build/src -lc++

# determine the name of the duckdb library that is built
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	DUCKDB_LIB = libduckdb.dylib
endif
ifeq ($(UNAME_S),Linux)
	DUCKDB_LIB = libduckdb.so
endif

all: duckdb $(OBJS)

include $(PGXS)

duckdb: third_party/duckdb third_party/build/src/$(DUCKDB_LIB)

third_party/duckdb:
	git submodule update --init --recursive

third_party/build/src/$(DUCKDB_LIB):
	cd third_party && \
	mkdir build && \
	cd build && \
	cmake ../duckdb && \
	make

install_duckdb:
	$(install_bin) -m 755 third_party/build/src/$(DUCKDB_LIB) $(DESTDIR)$(PG_LIB)


install: install_duckdb
