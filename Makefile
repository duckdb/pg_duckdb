.PHONY: duckdb install_duckdb clean_duckdb lintcheck .depend

MODULE_big = quack
EXTENSION = quack
DATA = quack.control $(wildcard quack--*.sql)

SRCS = src/quack_detoast.cpp \
	   src/quack_filter.cpp \
	   src/quack_heap_scan.cpp \
	   src/quack_heap_seq_scan.cpp \
	   src/quack_hooks.cpp \
	   src/quack_select.cpp \
	   src/quack_types.cpp \
	   src/quack_memory_allocator.cpp \
	   src/quack.cpp

OBJS = $(subst .cpp,.o, $(SRCS))

REGRESS = basic

PG_CONFIG ?= pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_LIB := $(shell $(PG_CONFIG) --pkglibdir)
INCLUDEDIR := ${shell $(PG_CONFIG) --includedir}
INCLUDEDIR_SERVER := ${shell $(PG_CONFIG) --includedir-server}

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

SHLIB_LINK += -Wl,-rpath,$(PG_LIB)/ -lpq -L$(PG_LIB) -lduckdb -Lthird_party/duckdb/build/$(QUACK_BUILD_DUCKDB)/src -lstdc++

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

all: duckdb $(OBJS)

include $(PGXS)

duckdb: third_party/duckdb/Makefile third_party/duckdb/build/$(QUACK_BUILD_DUCKDB)/src/$(DUCKDB_LIB)

third_party/duckdb/Makefile:
	git submodule update --init --recursive

third_party/duckdb/build/$(QUACK_BUILD_DUCKDB)/src/$(DUCKDB_LIB):
	$(MAKE) -C third_party/duckdb $(QUACK_BUILD_DUCKDB) DISABLE_SANITIZER=1 ENABLE_UBSAN=0 BUILD_UNITTESTS=OFF CMAKE_EXPORT_COMPILE_COMMANDS=1

install_duckdb:
	$(install_bin) -m 755 third_party/duckdb/build/$(QUACK_BUILD_DUCKDB)/src/$(DUCKDB_LIB) $(DESTDIR)$(PG_LIB)

clean_duckdb:
	rm -rf third_party/duckdb/build

install: install_duckdb

clean: clean_duckdb

lintcheck:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -Iinclude $(CPPFLAGS) -std=c++17

.depend:
	$(RM) -f .depend
	$(foreach SRC,$(SRCS),$(CXX) $(CPPFLAGS) -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -MM -MT $(SRC:.cpp=.o) $(SRC) >> .depend;)

include .depend
