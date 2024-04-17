.PHONY: duckdb install_duckdb clean_duckdb lintcheck

MODULE_big = quack
EXTENSION = quack
DATA = quack.control $(wildcard quack--*.sql)

SRCS = src/quack_heap_scan.cpp \
			src/quack_hooks.cpp \
			src/quack_select.cpp \
			src/quack_types.cpp \
			src/quack.cpp \
			src/scanner.cpp

OBJS = $(subst .cpp,.o, $(SRCS))

REGRESS = create_extension

PG_CONFIG ?= pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_LIB := $(shell $(PG_CONFIG) --pkglibdir)
INCLUDEDIR := ${shell $(PG_CONFIG) --includedir}
INCLUDEDIR_SERVER := ${shell $(PG_CONFIG) --includedir-server}

HEADERS := include/quack/scanner.hpp $(wildcard third_party/duckdb/src/include/*/*/*/*/*/*)

DEBUG_FLAGS = -g -O0 -fsanitize=address
override PG_CPPFLAGS += -Iinclude -Ithird_party/duckdb/src/include -std=c++17 -rdynamic

SHLIB_LINK += -Wl,-rpath,$(PG_LIB)/ -lpq -L$(PG_LIB) -lduckdb -Lthird_party/duckdb/build/debug/src -lstdc++

COMPILE.cpp.bc = $(CXX) -Wno-ignored-attributes -Wno-register $(BITCODE_CXXFLAGS) $(CXXFLAGS) $(PG_CPPFLAGS) -I$(INCLUDEDIR_SERVER) -emit-llvm -c

%.bc : %.cpp
	$(COMPILE.cpp.bc) $(SHLIB_LINK) $(PG_CPPFLAGS) -I$(INCLUDE_SERVER) -o $@ $<

# determine the name of the duckdb library that is built
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	DUCKDB_LIB = libduckdb.dylib
endif
ifeq ($(UNAME_S),Linux)
	DUCKDB_LIB = libduckdb.so
endif

all: duckdb $(OBJS) heap.so

include $(PGXS)

duckdb: third_party/duckdb third_party/duckdb/build/debug/src/$(DUCKDB_LIB)

heap.so:
	$(CXX) $(PG_CPPFLAGS) $(CPPFLAGS)  -Wl,-rpath,$(PG_LIB)/ -std=c++17 -shared src/scanner/heap.cpp -undefined dynamic_lookup -o heap.so

third_party/duckdb:
	git submodule update --init --recursive

third_party/duckdb/build/debug/src/$(DUCKDB_LIB):
	$(MAKE) -C third_party/duckdb debug DISABLE_SANITIZER=1

install_duckdb:
	$(install_bin) -m 755 third_party/duckdb/build/debug/src/$(DUCKDB_LIB) $(DESTDIR)$(PG_LIB)

clean_duckdb:
	rm -rf third_party/duckdb/build

install_heap_scanner:
	$(install_bin) -m 755 heap.so $(DESTDIR)$(PG_LIB)

install: install_duckdb install_heap_scanner

clean_heap_so:
	rm -f heap.so

clean_depend:
	rm -f .depend

clean: clean_duckdb clean_heap_so clean_depend


lintcheck:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -Iinclude $(CPPFLAGS) -std=c++17

.depend:
	$(RM) -f .depend
	$(foreach SRC,$(SRCS),$(CXX) $(CPPFLAGS) -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -MM -MT $(SRC:.cpp=.o) $(SRC) >> .depend;)

include .depend
