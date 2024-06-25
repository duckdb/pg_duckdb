.PHONY: duckdb install-duckdb clean-duckdb lintcheck check-regression-quack clean-regression .depend

MODULE_big = quack
EXTENSION = quack
DATA = quack.control $(wildcard sql/quack--*.sql)

SRCS = src/utility/copy.cpp \
	   src/quack_detoast.cpp \
	   src/quack_duckdb_connection.cpp \
	   src/quack_filter.cpp \
	   src/quack_heap_scan.cpp \
	   src/quack_heap_seq_scan.cpp \
	   src/quack_hooks.cpp \
	   src/quack_memory_allocator.cpp \
	   src/quack_node.cpp \
	   src/quack_planner.cpp \
	   src/quack_types.cpp \
	   src/quack.cpp

OBJS = $(subst .cpp,.o, $(SRCS))

QUACK_BUILD_CXX_FLAGS=
QUACK_BUILD_DUCKDB=

ifeq ($(QUACK_BUILD), Debug)
	QUACK_BUILD_CXX_FLAGS = -g -O0
	QUACK_BUILD_DUCKDB = debug
else
	QUACK_BUILD_CXX_FLAGS =
	QUACK_BUILD_DUCKDB = release
endif

override PG_CPPFLAGS += -Iinclude -Ithird_party/duckdb/src/include -std=c++17 -Wno-sign-compare ${QUACK_BUILD_CXX_FLAGS}

SHLIB_LINK += -Wl,-rpath,$(PG_LIB)/ -lpq -L$(PG_LIB) -lduckdb -Lthird_party/duckdb/build/$(QUACK_BUILD_DUCKDB)/src -lstdc++ -llz4

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

check-regression-quack:
	$(MAKE) -C test/regression check-regression-quack

clean-regression:
	$(MAKE) -C test/regression clean-regression

installcheck: all install check-regression-quack

duckdb: third_party/duckdb/Makefile third_party/duckdb/build/$(QUACK_BUILD_DUCKDB)/src/$(DUCKDB_LIB)

third_party/duckdb/Makefile:
	git submodule update --init --recursive

third_party/duckdb/build/$(QUACK_BUILD_DUCKDB)/src/$(DUCKDB_LIB):
	$(MAKE) -C third_party/duckdb \
	$(QUACK_BUILD_DUCKDB) \
	DISABLE_SANITIZER=1 \
	ENABLE_UBSAN=0 \
	BUILD_UNITTESTS=OFF \
	BUILD_HTTPFS=1 \
	BUILD_JSON=1 \
	CMAKE_EXPORT_COMPILE_COMMANDS=1 \
	-j8

install-duckdb:
	$(install_bin) -m 755 third_party/duckdb/build/$(QUACK_BUILD_DUCKDB)/src/$(DUCKDB_LIB) $(DESTDIR)$(PG_LIB)

clean-duckdb:
	rm -rf third_party/duckdb/build

install: install-duckdb

clean: clean-regression clean-duckdb

lintcheck:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -Iinclude $(CPPFLAGS) -std=c++17

.depend:
	$(RM) -f .depend
	$(foreach SRC,$(SRCS),$(CXX) $(CPPFLAGS) -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -MM -MT $(SRC:.cpp=.o) $(SRC) >> .depend;)

include .depend
