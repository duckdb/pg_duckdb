.PHONY: duckdb install-duckdb clean-duckdb clean-all lintcheck check-regression-duckdb clean-regression

MODULE_big = pg_duckdb
EXTENSION = pg_duckdb
DATA = pg_duckdb.control $(wildcard sql/pg_duckdb--*.sql)

SRCS = $(wildcard src/*.cpp src/*/*.cpp)
OBJS = $(subst .cpp,.o, $(SRCS))

C_SRCS = $(wildcard src/*.c src/*/*.c)
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

DUCKDB_LIB = libduckdb$(DLSUFFIX)
FULL_DUCKDB_LIB = third_party/duckdb/build/$(DUCKDB_BUILD_TYPE)/src/$(DUCKDB_LIB)

override PG_CPPFLAGS += -Iinclude -Ithird_party/duckdb/src/include -Ithird_party/duckdb/third_party/re2
override PG_CXXFLAGS += -std=c++17 -Wno-sign-compare -Wno-register ${DUCKDB_BUILD_CXX_FLAGS}

SHLIB_LINK += -Wl,-rpath,$(PG_LIB)/ -lpq -Lthird_party/duckdb/build/$(DUCKDB_BUILD_TYPE)/src -L$(PG_LIB) -lduckdb -lstdc++ -llz4

COMPILE.cc.bc = $(CXX) -Wno-ignored-attributes -Wno-register $(BITCODE_CXXFLAGS) $(CXXFLAGS) $(PG_CPPFLAGS) $(PG_CXXFLAGS) -I$(INCLUDEDIR_SERVER) -emit-llvm -c
COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes $(BITCODE_CXXFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) $(PG_CXXFLAGS) -flto=thin -emit-llvm -c

include Makefile.global

# We need the DuckDB header files to build the .o files. We depend on the
# duckdb Makefile, because that target pulls in the submodule which includes
# those header files.
$(OBJS): third_party/duckdb/Makefile

# shlib is the final output product - make duckdb and all .o dependencies
$(shlib): $(FULL_DUCKDB_LIB) $(OBJS)

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

duckdb: $(FULL_DUCKDB_LIB)

third_party/duckdb/Makefile:
	git submodule update --init --recursive

$(FULL_DUCKDB_LIB): third_party/duckdb/Makefile
	$(MAKE) -C third_party/duckdb \
	$(DUCKDB_BUILD_TYPE) \
	DISABLE_SANITIZER=1 \
	ENABLE_UBSAN=0 \
	BUILD_UNITTESTS=OFF \
	EXTENSION_CONFIGS="../pg_duckdb_extensions.cmake"

install-duckdb: $(FULL_DUCKDB_LIB) $(shlib)
	$(install_bin) -m 755 $(FULL_DUCKDB_LIB) $(DESTDIR)$(PG_LIB)

clean-duckdb:
	rm -rf third_party/duckdb/build

install: install-duckdb

clean-all: clean clean-regression clean-duckdb

lintcheck:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -Iinclude $(CPPFLAGS) -std=c++17
	ruff check

format:
	git clang-format origin/main
	ruff format
