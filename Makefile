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
	   src/vendor/pg_explain.cpp \
	   src/pgduckdb_metadata_cache.cpp \
	   src/pgduckdb_detoast.cpp \
	   src/pgduckdb_duckdb.cpp \
	   src/pgduckdb_filter.cpp \
	   src/pgduckdb_hooks.cpp \
	   src/pgduckdb_memory_allocator.cpp \
	   src/pgduckdb_node.cpp \
	   src/pgduckdb_options.cpp \
	   src/pgduckdb_planner.cpp \
	   src/pgduckdb_ruleutils.cpp \
	   src/pgduckdb_types.cpp \
	   src/pgduckdb.cpp

OBJS = $(subst .cpp,.o, $(SRCS))


C_SRCS = src/vendor/pg_ruleutils_16.c \
		 src/vendor/pg_ruleutils_17.c
OBJS += $(subst .c,.o, $(C_SRCS))


DUCKDB_BUILD_CXX_FLAGS=
DUCKDB_BUILD_TYPE=

ifeq ($(DUCKDB_BUILD), Debug)
	DUCKDB_BUILD_CXX_FLAGS = -g -O0
	DUCKDB_BUILD_TYPE = debug
else
	DUCKDB_BUILD_CXX_FLAGS =
	DUCKDB_BUILD_TYPE = release
endif

override PG_CPPFLAGS += -Iinclude -Ithird_party/duckdb/src/include -Ithird_party/duckdb/third_party/re2
override PG_CXXFLAGS += -std=c++17 -Wno-sign-compare ${DUCKDB_BUILD_CXX_FLAGS}

SHLIB_LINK += -Wl,-rpath,$(PG_LIB)/ -lpq -Lthird_party/duckdb/build/$(DUCKDB_BUILD_TYPE)/src -L$(PG_LIB) -lduckdb -lstdc++ -llz4

COMPILE.cc.bc = $(CXX) -Wno-ignored-attributes -Wno-register $(BITCODE_CXXFLAGS) $(CXXFLAGS) $(PG_CPPFLAGS) $(PG_CXXFLAGS) -I$(INCLUDEDIR_SERVER) -emit-llvm -c

%.bc : %.cpp
	$(COMPILE.cc.bc) $(SHLIB_LINK) -I$(INCLUDE_SERVER) -o $@ $<

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

PYTEST_CONCURRENCY = auto

check-regression-duckdb:
	$(MAKE) -C test/regression check-regression-duckdb

clean-regression:
	$(MAKE) -C test/regression clean-regression

installcheck: all install
	$(MAKE) check-regression-duckdb

pycheck: all install
	pytest -n $(PYTEST_CONCURRENCY)

check: installcheck pycheck

FULL_DUCKDB_LIB = third_party/duckdb/build/$(DUCKDB_BUILD_TYPE)/src/$(DUCKDB_LIB)
duckdb: third_party/duckdb/Makefile $(FULL_DUCKDB_LIB)


third_party/duckdb/Makefile:
	git submodule update --init --recursive

$(FULL_DUCKDB_LIB):
	$(MAKE) -C third_party/duckdb \
	$(DUCKDB_BUILD_TYPE) \
	DISABLE_SANITIZER=1 \
	ENABLE_UBSAN=0 \
	BUILD_UNITTESTS=OFF \
	EXTENSION_CONFIGS="../pg_duckdb_extensions.cmake"

install-duckdb: $(FULL_DUCKDB_LIB)
	$(install_bin) -m 755 $(FULL_DUCKDB_LIB) $(DESTDIR)$(PG_LIB)

clean-duckdb:
	rm -rf third_party/duckdb/build

install: install-duckdb

clean: clean-regression clean-duckdb

lintcheck:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -Iinclude $(CPPFLAGS) -std=c++17
	ruff check

.depend:
	$(RM) -f .depend
	$(foreach SRC,$(SRCS),$(CXX) $(CPPFLAGS) -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -MM -MT $(SRC:.cpp=.o) $(SRC) >> .depend;)

format:
	git clang-format origin/main
	ruff format

include .depend
