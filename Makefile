.PHONY: duckdb install-duckdb clean-duckdb lintcheck check-regression-duckdb clean-regression .depend

MODULE_big = pg_duckdb
EXTENSION = pg_duckdb
DATA = pg_duckdb.control $(wildcard sql/pg_duckdb--*.sql)

SRCS = src/scan/heap_reader.cpp \
	   src/scan/index_scan_utils.cpp \
	   src/scan/postgres_index_scan.cpp \
	   src/scan/postgres_scan.cpp \
	   src/scan/postgres_seq_scan.cpp \
	   src/utility/copy.cpp \
	   src/pgduckdb_background_worker.cpp \
	   src/pgduckdb_ddl.cpp \
	   src/pgduckdb_detoast.cpp \
	   src/pgduckdb_duckdb.cpp \
	   src/pgduckdb_filter.cpp \
	   src/pgduckdb_hooks.cpp \
	   src/pgduckdb_memory_allocator.cpp \
	   src/pgduckdb_node.cpp \
	   src/pgduckdb_options.cpp \
	   src/pgduckdb_planner.cpp \
	   src/pgduckdb_table_am.cpp \
	   src/pgduckdb_types.cpp \
	   src/pgduckdb.cpp

OBJS = $(subst .cpp,.o, $(SRCS))

DUCKDB_BUILD_CXX_FLAGS=
DUCKDB_BUILD_TYPE=

ifeq ($(DUCKDB_BUILD), Debug)
	DUCKDB_BUILD_CXX_FLAGS = -g -O0
	DUCKDB_BUILD_TYPE = debug
else
	DUCKDB_BUILD_CXX_FLAGS =
	DUCKDB_BUILD_TYPE = release
endif

override PG_CPPFLAGS += -Iinclude -Ithird_party/duckdb/src/include -Ithird_party/duckdb/third_party/re2 -std=c++17 -Wno-sign-compare ${DUCKDB_BUILD_CXX_FLAGS}

SHLIB_LINK += -Wl,-rpath,$(PG_LIB)/ -lpq -L$(PG_LIB) -lduckdb -Lthird_party/duckdb/build/$(DUCKDB_BUILD_TYPE)/src -lstdc++ -llz4

COMPILE.cc.bc = $(CXX) -Wno-ignored-attributes -Wno-register $(BITCODE_CXXFLAGS) $(CXXFLAGS) $(PG_CPPFLAGS) -I$(INCLUDEDIR_SERVER) -emit-llvm -c

%.bc : %.cpp
	$(COMPILE.cc.bc) $(SHLIB_LINK) $(PG_CPPFLAGS) -I$(INCLUDE_SERVER) -o $@ $<

# determine the name of the duckdb library that is built
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	DUCKDB_LIB = libduckdb.dylib
endif
ifeq ($(UNAME_S),Linux)
	DUCKDB_LIB = libduckdb.so
endif

all: duckdb $(OBJS) .depend

include Makefile.global

NO_INSTALLCHECK = 1

check-regression-duckdb:
	$(MAKE) -C test/regression check-regression-duckdb

clean-regression:
	$(MAKE) -C test/regression clean-regression

installcheck: all install check-regression-duckdb

duckdb: third_party/duckdb/Makefile third_party/duckdb/build/$(DUCKDB_BUILD_TYPE)/src/$(DUCKDB_LIB)

third_party/duckdb/Makefile:
	git submodule update --init --recursive

third_party/duckdb/build/$(DUCKDB_BUILD_TYPE)/src/$(DUCKDB_LIB):
	$(MAKE) -C third_party/duckdb \
	$(DUCKDB_BUILD_TYPE) \
	DISABLE_SANITIZER=1 \
	ENABLE_UBSAN=0 \
	BUILD_UNITTESTS=OFF \
	BUILD_HTTPFS=1 \
	BUILD_JSON=1 \
	CMAKE_EXPORT_COMPILE_COMMANDS=1 \
	-j8

install-duckdb:
	$(install_bin) -m 755 third_party/duckdb/build/$(DUCKDB_BUILD_TYPE)/src/$(DUCKDB_LIB) $(DESTDIR)$(PG_LIB)

clean-duckdb:
	rm -rf third_party/duckdb/build

install: install-duckdb

clean: clean-regression clean-duckdb

lintcheck:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -Iinclude $(CPPFLAGS) -std=c++17

.depend:
	$(RM) -f .depend
	$(foreach SRC,$(SRCS),$(CXX) $(CPPFLAGS) -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -MM -MT $(SRC:.cpp=.o) $(SRC) >> .depend;)

format:
	git clang-format origin/main

include .depend
