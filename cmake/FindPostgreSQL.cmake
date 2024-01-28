# Copyright 2020 Mats Kindahl
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# .rst: FindPostgreSQL
# --------------------
#
# Find the PostgreSQL installation.
#
# This module defines the following variables
#
# ::
#
# PostgreSQL_LIBRARIES - the PostgreSQL libraries needed for linking
#
# PostgreSQL_INCLUDE_DIRS - include directories
#
# PostgreSQL_SERVER_INCLUDE_DIRS - include directories for server programming
#
# PostgreSQL_LIBRARY_DIRS  - link directories for PostgreSQL libraries
#
# PostgreSQL_EXTENSION_DIR  - the directory for extensions
#
# PostgreSQL_SHARED_LINK_OPTIONS  - options for shared libraries
#
# PostgreSQL_LINK_OPTIONS  - options for static libraries and executables
#
# PostgreSQL_VERSION_STRING - the version of PostgreSQL found (since CMake
# 2.8.8)
#
# ----------------------------------------------------------------------------
# History: This module is derived from the existing FindPostgreSQL.cmake and try
# to use most of the existing output variables of that module, but uses
# `pg_config` to extract the necessary information instead and add a macro for
# creating extensions. The use of `pg_config` is aligned with how the PGXS code
# distributed with PostgreSQL itself works.

# Define additional search paths for root directories.
set(PostgreSQL_ROOT_DIRECTORIES ENV PGROOT ENV PGPATH ${PostgreSQL_ROOT})

if (DEFINED ENV{PG_CONFIG})
  set(PG_CONFIG "$ENV{PG_CONFIG}")
else()
  find_program(
    PG_CONFIG pg_config
    PATHS ${PostgreSQL_ROOT_DIRECTORIES}
    PATH_SUFFIXES bin)
endif()

if(NOT PG_CONFIG)
  message(FATAL_ERROR "Could not find pg_config")
else()
  set(PostgreSQL_FOUND TRUE)
endif()

message(STATUS "Found pg_config as ${PG_CONFIG}")

if(PostgreSQL_FOUND)
  macro(PG_CONFIG VAR OPT)
    execute_process(
      COMMAND ${PG_CONFIG} ${OPT}
      OUTPUT_VARIABLE ${VAR}
      OUTPUT_STRIP_TRAILING_WHITESPACE)
  endmacro()

  pg_config(_pg_bindir --bindir)
  pg_config(_pg_includedir --includedir)
  pg_config(_pg_pkgincludedir --pkgincludedir)
  pg_config(_pg_sharedir --sharedir)
  pg_config(_pg_includedir_server --includedir-server)
  pg_config(_pg_libs --libs)
  pg_config(_pg_ldflags --ldflags)
  pg_config(_pg_ldflags_sl --ldflags_sl)
  pg_config(_pg_ldflags_ex --ldflags_ex)
  pg_config(_pg_pkglibdir --pkglibdir)
  pg_config(_pg_libdir --libdir)
  pg_config(_pg_version --version)
  pg_config(_pg_compileflags --configure)
  pg_config(_pg_cflags --cflags)
  pg_config(_pg_cflags_sl --cflags_sl)
  pg_config(_pg_cxxflags --cppflags)

  separate_arguments(_pg_ldflags)
  separate_arguments(_pg_ldflags_sl)
  separate_arguments(_pg_ldflags_ex)

  set(_server_lib_dirs ${_pg_libdir} ${_pg_pkglibdir})
  set(_server_inc_dirs ${_pg_includedir_server} ${_pg_pkgincludedir})
  string(REPLACE ";" " " _shared_link_options
                 "${_pg_ldflags};${_pg_ldflags_sl}")


  set(_link_options ${_pg_ldflags})
  if(_pg_ldflags_ex)
    list(APPEND _link_options ${_pg_ldflags_ex})
  endif()

  string(FIND ${_pg_compileflags} "--with-llvm" _pg_with_llvm_idx)

  if(${_pg_with_llvm_idx} EQUAL -1)
	  set(_pg_with_llvm FALSE)
  else()
	  set(_pg_with_llvm TRUE)
  endif()

  set(PostgreSQL_INCLUDE_DIRS
      "${_pg_includedir}"
      CACHE PATH
            "Top-level directory containing the PostgreSQL include directories."
  )
  set(PostgreSQL_EXTENSION_DIR
      "${_pg_sharedir}/extension"
      CACHE PATH "Directory containing extension SQL and control files")
  set(PostgreSQL_SERVER_INCLUDE_DIRS
      "${_server_inc_dirs}"
      CACHE PATH "PostgreSQL include directories for server include files.")
  set(PostgreSQL_LIBRARY_DIRS
      "${_pg_libdir}"
      CACHE PATH "library directory for PostgreSQL")
  set(PostgreSQL_LIBRARIES
      "${_pg_libs}"
      CACHE PATH "Libraries for PostgreSQL")
  set(PostgreSQL_SHARED_LINK_OPTIONS
      "${_shared_link_options}"
      CACHE STRING "PostgreSQL linker options for shared libraries.")
  set(PostgreSQL_LINK_OPTIONS
      "${_pg_ldflags},${_pg_ldflags_ex}"
      CACHE STRING "PostgreSQL linker options for executables.")
  set(PostgreSQL_SERVER_LIBRARY_DIRS
      "${_server_lib_dirs}"
      CACHE PATH "PostgreSQL server library directories.")
  set(PostgreSQL_VERSION_STRING
      "${_pg_version}"
      CACHE STRING "PostgreSQL version string")
  set(PostgreSQL_PACKAGE_LIBRARY_DIR
      "${_pg_pkglibdir}"
      CACHE STRING "PostgreSQL package library directory")
  set(PostgreSQL_WITH_LLVM
      "${_pg_with_llvm}"
      CACHE BOOL "PostgreSQL -with-llvm flag.")
  set(PostgreSQL_CFLAGS
      "${_pg_cflags}"
      CACHE STRING "PostgreSQL CFLAGS")
  set(PostgreSQL_CFLAGS_SL
      "${_pg_cflags_sl}"
      CACHE STRING "PostgreSQL CFLAGS_SL")
  set(PostgreSQL_CXXFLAGS
      "${_pg_cxxflags}"
      CACHE STRING "PostgreSQL CXXFLAGS")

  find_program(
    PG_BINARY postgres
    PATHS ${PostgreSQL_ROOT_DIRECTORIES}
    HINTS ${_pg_bindir}
    PATH_SUFFIXES bin)

  if(NOT PG_BINARY)
    message(FATAL_ERROR "Could not find postgres binary")
  endif()

  message(STATUS "Found postgres binary at ${PG_BINARY}")

  find_program(PG_REGRESS pg_regress HINT
               ${PostgreSQL_PACKAGE_LIBRARY_DIR}/pgxs/src/test/regress)
  if(NOT PG_REGRESS)
    message(STATUS "Could not find pg_regress, tests not executed")
  endif()

  message(STATUS "PostgreSQL version: ${PostgreSQL_VERSION_STRING} found")
  message(
    STATUS
      "PostgreSQL package library directory: ${PostgreSQL_PACKAGE_LIBRARY_DIR}")
  message(STATUS "PostgreSQL libraries: ${PostgreSQL_LIBRARIES}")
  message(STATUS "PostgreSQL extension directory: ${PostgreSQL_EXTENSION_DIR}")
  message(STATUS "PostgreSQL linker options: ${PostgreSQL_LINK_OPTIONS}")
  message(
    STATUS "PostgreSQL shared linker options: ${PostgreSQL_SHARED_LINK_OPTIONS}"
  )
endif()