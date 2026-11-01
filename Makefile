.PHONY: duckdb install-duckdb clean-duckdb clean-all lintcheck check-regression-duckdb clean-regression

PG_DUCKDB_VERSION ?= $(shell git describe --always --dirty 2>/dev/null || echo "unknown")

MODULE_big = pg_duckdb
EXTENSION = pg_duckdb
DATA = pg_duckdb.control $(wildcard sql/pg_duckdb--*.sql)

SRCS = $(wildcard src/*.cpp src/*/*.cpp)
OBJS = $(subst .cpp,.o, $(SRCS))

C_SRCS = $(wildcard src/*.c src/*/*.c)
OBJS += $(subst .c,.o, $(C_SRCS))

# set to `make` to disable ninja
DUCKDB_GEN ?= ninja
# used to know what version of extensions to download
DUCKDB_VERSION = v2.0.0
# duckdb build tweaks
DUCKDB_CMAKE_VARS = -DCXX_EXTRA=-fvisibility=default -DBUILD_SHELL=0 -DBUILD_PYTHON=0 -DBUILD_UNITTESTS=0 -DOVERRIDE_GIT_DESCRIBE=$(DUCKDB_VERSION)
# set to 1 to disable asserts in DuckDB. This is particularly useful in combinition with MotherDuck.
# When asserts are enabled the released motherduck extension will fail some of
# those asserts. By disabling asserts it's possible to run a debug build of
# DuckDB agains the release build of MotherDuck.
DUCKDB_DISABLE_ASSERTIONS ?= 0

# DUCKDB_BUILD selects how DuckDB is built and linked, and where httpfs gets openssl and curl from:
#   Release (default)   - dynamically linked, system openssl and curl
#   Debug               - same as Release, with a debug build of DuckDB
#   ReleaseStatic       - statically linked, openssl and curl built by vcpkg and linked statically
#   ReleaseStaticSystem - statically linked, system openssl and curl
DUCKDB_BUILD_CXX_FLAGS=
DUCKDB_BUILD_TYPE = release
DUCKDB_MAKE_TARGET = release
DUCKDB_STATIC =
DUCKDB_VCPKG =
ifeq ($(DUCKDB_BUILD), Debug)
	DUCKDB_BUILD_CXX_FLAGS = -g -O0 -D_GLIBCXX_ASSERTIONS
	DUCKDB_BUILD_TYPE = debug
	DUCKDB_MAKE_TARGET = debug
else ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	DUCKDB_STATIC = 1
	DUCKDB_VCPKG = 1
else ifeq ($(DUCKDB_BUILD), ReleaseStaticSystem)
	DUCKDB_STATIC = 1
endif

DUCKDB_BUILD_DIR = third_party/duckdb/build/$(DUCKDB_BUILD_TYPE)

ifdef DUCKDB_VCPKG
	# Uses an installed vcpkg; the package versions are pinned by the builtin-baseline of DuckDB's vcpkg manifest
	ifeq ($(VCPKG_TOOLCHAIN_PATH),)
$(error DUCKDB_BUILD=$(DUCKDB_BUILD) requires VCPKG_TOOLCHAIN_PATH to point at the vcpkg.cmake of an installed vcpkg)
	endif
	DUCKDB_DEPS_ENV = DUCKDB_NEW_EXTENSION_BUILD=1 VCPKG_TOOLCHAIN_PATH=$(VCPKG_TOOLCHAIN_PATH) \
		VCPKG_TARGET_TRIPLET=$(VCPKG_TARGET_TRIPLET)
	# vcpkg installs the libraries into the build directory while DuckDB is built, so look them up lazily. The static
	# curl needs more libraries (zlib, frameworks on macOS), which its pkg-config file lists. Link the ones vcpkg built
	# by path, so that -L flags of the Postgres build cannot make the linker pick up system versions of them.
	VCPKG_LIB_DIR = $(patsubst %/libcurl.a,%,$(firstword $(wildcard $(DUCKDB_BUILD_DIR)/vcpkg_installed/$(or $(VCPKG_TARGET_TRIPLET),*)/lib/libcurl.a)))
	VCPKG_CURL_LIBS = $(shell PKG_CONFIG_LIBDIR=$(VCPKG_LIB_DIR)/pkgconfig pkg-config --static --libs libcurl)
	DUCKDB_DEPS_LINK_FLAGS = $(foreach flag,$(filter-out -L%,$(VCPKG_CURL_LIBS)),$(or $(wildcard $(patsubst -l%,$(VCPKG_LIB_DIR)/lib%.a,$(filter -l%,$(flag)))),$(flag)))
else
	# DuckDB's new extension build requires vcpkg for the dependencies of httpfs, the legacy one finds them on the
	# system. Clear the vcpkg variables so that a vcpkg set up in the environment is not picked up halfway.
	DUCKDB_DEPS_ENV = DUCKDB_NEW_EXTENSION_BUILD=0 VCPKG_TOOLCHAIN_PATH= VCPKG_TARGET_TRIPLET=
	DUCKDB_DEPS_LINK_FLAGS = -lssl -lcrypto -lcurl
	ifeq ($(shell uname -s), Darwin)
		# httpfs loads the system certificates through these
		DUCKDB_DEPS_LINK_FLAGS += -framework CoreFoundation -framework Security
	endif
endif

ifdef DUCKDB_STATIC
	DUCKDB_CMAKE_VARS += -DPG_DUCKDB_STATIC_HTTPFS=ON
	FULL_DUCKDB_LIB = $(DUCKDB_BUILD_DIR)/src/libduckdb_static.a
	# registers the statically linked extensions when pg_duckdb is loaded
	DUCKDB_STATIC_LOADER = $(addprefix $(DUCKDB_BUILD_DIR)/src/CMakeFiles/duckdb.dir/,duckdb_static_extension_loader.c.o __/extension/loader/static_extension_autoregister.cpp.o)
	PG_DUCKDB_LINK_FLAGS = $(DUCKDB_STATIC_LOADER) $(wildcard $(DUCKDB_BUILD_DIR)/extension/*/lib*_extension.a) \
		$(DUCKDB_BUILD_DIR)/src/main/http/libduckdb_httplib.a $(DUCKDB_BUILD_DIR)/src/main/extension/libduckdb_loadable_extensions.a \
		$(FULL_DUCKDB_LIB) $(DUCKDB_DEPS_LINK_FLAGS)
else
	DUCKDB_CMAKE_VARS += -DPG_DUCKDB_STATIC_HTTPFS=OFF
	FULL_DUCKDB_LIB = $(DUCKDB_BUILD_DIR)/src/libduckdb$(DLSUFFIX)
	PG_DUCKDB_LINK_FLAGS = -lduckdb
endif


PG_DUCKDB_LINK_FLAGS += -Wl,-rpath,$(PG_LIB)/ -L$(DUCKDB_BUILD_DIR)/src -L$(PG_LIB) -lstdc++ -llz4

# Ensure -lstdc++fs is included for GCC 8 builds
CXX ?= c++
IS_GCC := $(shell $(CXX) --version 2>/dev/null | grep -q "Free Software Foundation" && echo true || echo false)
ifeq ($(IS_GCC),true)
  GCC_MAJOR := $(shell $(CXX) -dumpversion 2>/dev/null | cut -d. -f1)
  ifeq ($(GCC_MAJOR),8)
    PG_DUCKDB_LINK_FLAGS += -lstdc++fs
  endif
endif

ERROR_ON_WARNING ?=
ifeq ($(ERROR_ON_WARNING), 1)
	ERROR_ON_WARNING = -Werror
else
	ERROR_ON_WARNING =
endif

COMPILER_FLAGS=-Wno-sign-compare -Wshadow -Wswitch -Wunused-parameter -Wunreachable-code -Wno-unknown-pragmas -Wall -Wextra -Wno-missing-field-initializers ${ERROR_ON_WARNING}

override PG_CPPFLAGS += -Iinclude -isystem third_party/duckdb/src/include -isystem third_party/duckdb/third_party/re2 -isystem third_party/duckdb/third_party/fmt/include -isystem $(INCLUDEDIR_SERVER) ${COMPILER_FLAGS}
override PG_CXXFLAGS += -std=c++17 ${DUCKDB_BUILD_CXX_FLAGS} ${COMPILER_FLAGS} -Wno-register -Weffc++
# Ignore declaration-after-statement warnings in our code. Postgres enforces
# this because their ancient style guide requires it, but we don't care. It
# would only apply to C files anyway, and we don't have many of those. The only
# ones that we do have are vendored in from Postgres (ruleutils), and allowing
# declarations to be anywhere is even a good thing for those as we can keep our
# changes to the vendored code in one place.
override PG_CFLAGS += -Wno-declaration-after-statement

SHLIB_LINK += $(PG_DUCKDB_LINK_FLAGS)

include Makefile.global

# Only pass the version define to the one file that needs it, so that ccache
# doesn't invalidate everything on every commit.
src/pgduckdb.o: PG_CPPFLAGS += -DPG_DUCKDB_VERSION="\"$(PG_DUCKDB_VERSION)\""

# We need the DuckDB header files to build our own .o files. We depend on the
# duckdb submodule HEAD, because that target pulls in the submodule which
# includes those header files. This does mean that we rebuild our .o files
# whenever we change the DuckDB version, but that seems like a fairly
# reasonable thing to do anyway, even if not always strictly necessary always.
$(OBJS): .git/modules/third_party/duckdb/HEAD

COMPILE.cc.bc += $(PG_CPPFLAGS)
COMPILE.cxx.bc += $(PG_CXXFLAGS)

# shlib is the final output product - make duckdb and all .o dependencies
$(shlib): $(FULL_DUCKDB_LIB) $(OBJS)

NO_INSTALLCHECK = 1

PYTEST_CONCURRENCY = auto

check-regression-duckdb:
	$(MAKE) -C test/regression check-regression-duckdb

clean-regression:
	$(MAKE) -C test/regression clean-regression

# The tests install httpfs from the extension repository of the DuckDB build, instead of downloading it
DUCKDB_EXTENSION_REPOSITORY = $(abspath $(DUCKDB_BUILD_DIR)/repository)

# Specify AWS_REGION to make sure test output the same thing regardless of where they are run
installcheck: all install
	AWS_REGION=us-east-1 PG_DUCKDB_EXTENSION_REPOSITORY=$(DUCKDB_EXTENSION_REPOSITORY) $(MAKE) check-regression-duckdb

pycheck: all install
	LD_LIBRARY_PATH=$(PG_LIBDIR):${LD_LIBRARY_PATH} PG_DUCKDB_EXTENSION_REPOSITORY=$(DUCKDB_EXTENSION_REPOSITORY) \
		pytest -n $(PYTEST_CONCURRENCY)

check: installcheck pycheck schedulecheck

schedulecheck:
	./scripts/schedule-check.sh

duckdb: $(FULL_DUCKDB_LIB)

.git/modules/third_party/duckdb/HEAD:
	git submodule update --init --recursive

$(FULL_DUCKDB_LIB): .git/modules/third_party/duckdb/HEAD third_party/pg_duckdb_extensions.cmake
	OVERRIDE_GIT_DESCRIBE=$(DUCKDB_VERSION) \
	$(DUCKDB_DEPS_ENV) \
	DUCKDB_CAPABILITIES="httplib;loadable_extensions" \
	GEN=$(DUCKDB_GEN) \
	CMAKE_VARS="$(DUCKDB_CMAKE_VARS)" \
	DISABLE_SANITIZER=1 \
	DISABLE_ASSERTIONS=$(DUCKDB_DISABLE_ASSERTIONS) \
	EXTENSION_CONFIGS="../pg_duckdb_extensions.cmake" \
	$(MAKE) -C third_party/duckdb \
	$(DUCKDB_MAKE_TARGET)
	# DuckDB's build leaves the library untouched when it is up to date, which would make this rule run every time
	touch $@

ifdef DUCKDB_STATIC
install-duckdb: $(FULL_DUCKDB_LIB) $(shlib)
else
install-duckdb: $(FULL_DUCKDB_LIB) $(shlib)
	$(install_bin) -m 755 $(FULL_DUCKDB_LIB) $(DESTDIR)$(PG_LIB)
endif

clean-duckdb:
	rm -rf third_party/duckdb/build

install: install-duckdb

clean-all: clean clean-regression clean-duckdb

lintcheck:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -Iinclude $(CPPFLAGS) -std=c++17
	ruff check

format:
	find src include -iname '*.hpp' -o -iname '*.h' -o -iname '*.cpp' -o -iname '*.c' | xargs git clang-format origin/main
	ruff format

format-all:
	find src include -iname '*.hpp' -o -iname '*.h' -o -iname '*.cpp' -o -iname '*.c' | xargs clang-format -i
	ruff format
