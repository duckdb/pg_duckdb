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

find_program(
  PG_CONFIG pg_config
  PATHS ${PostgreSQL_ROOT_DIRECTORIES}
  PATH_SUFFIXES bin)

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

  separate_arguments(_pg_ldflags)
  separate_arguments(_pg_ldflags_sl)
  separate_arguments(_pg_ldflags_ex)

  set(_server_lib_dirs ${_pg_libdir} ${_pg_pkglibdir})
  set(_server_inc_dirs ${_pg_pkgincludedir} ${_pg_includedir_server})
  string(REPLACE ";" " " _shared_link_options
                 "${_pg_ldflags};${_pg_ldflags_sl}")
  set(_link_options ${_pg_ldflags})
  if(_pg_ldflags_ex)
    list(APPEND _link_options ${_pg_ldflags_ex})
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

  message(STATUS "PostgreSQL version ${PostgreSQL_VERSION_STRING} found")
  message(
    STATUS
    "PostgreSQL package library directory: ${PostgreSQL_PACKAGE_LIBRARY_DIR}")
  message(STATUS "PostgreSQL libraries: ${PostgreSQL_LIBRARIES}")
  message(STATUS "PostgreSQL extension directory: ${PostgreSQL_EXTENSION_DIR}")
  message(STATUS "PostgreSQL linker options: ${PostgreSQL_LINK_OPTIONS}")
  message(
    STATUS "PostgreSQL shared linker options: ${PostgreSQL_SHARED_LINK_OPTIONS}")
endif()

# add_postgresql_extension(NAME ...)
#
# VERSION Version of the extension. Is used when generating the control file.
# Required.
#
# ENCODING Encoding for the control file. Optional.
#
# COMMENT Comment for the control file. Optional.
#
# SOURCES List of source files to compile for the extension.
#
# REQUIRES List of extensions that are required by this extension.
#
# SCRIPTS Script files.
#
# SCRIPT_TEMPLATES Template script files.
#
# REGRESS Regress tests.
function(add_postgresql_extension NAME)
  set(_optional)
  set(_single VERSION ENCODING)
  set(_multi SOURCES SCRIPTS SCRIPT_TEMPLATES REQUIRES REGRESS)
  cmake_parse_arguments(_ext "${_optional}" "${_single}" "${_multi}" ${ARGN})

  if(NOT _ext_VERSION)
    message(FATAL_ERROR "Extension version not set")
  endif()

  # Here we are assuming that there is at least one source file, which is
  # strictly speaking not necessary for an extension. If we do not have source
  # files, we need to create a custom target and attach properties to that. We
  # expect the user to be able to add target properties after creating the
  # extension.
  add_library(${NAME} MODULE ${_ext_SOURCES})


  set(_link_flags "${PostgreSQL_SHARED_LINK_OPTIONS}")
  foreach(_dir ${PostgreSQL_SERVER_LIBRARY_DIRS})
    set(_link_flags "${_link_flags} -L${_dir}")
  endforeach()

  # Collect and build script files to install
  set(_script_files ${_ext_SCRIPTS})
  foreach(_template ${_ext_SCRIPT_TEMPLATES})
    string(REGEX REPLACE "\.in$" "" _script ${_template})
    configure_file(${_template} ${_script} @ONLY)
    list(APPEND _script_files ${CMAKE_CURRENT_BINARY_DIR}/${_script})
    message(
      STATUS "Building script file ${_script} from template file ${_template}")
  endforeach()

  if(APPLE)
    set(_link_flags "${_link_flags} -bundle_loader ${PG_BINARY}")
  endif()

  set_target_properties(
    ${NAME}
    PROPERTIES PREFIX ""
               LINK_FLAGS "${_link_flags}"
               POSITION_INDEPENDENT_CODE ON)

  target_include_directories(
    ${NAME}
    PRIVATE ${PostgreSQL_SERVER_INCLUDE_DIRS}
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})


  # Generate control file at build time (which is when GENERATE evaluate the
  # contents). We do not know the target file name until then.
  set(_control_file "${CMAKE_CURRENT_BINARY_DIR}/${NAME}.control")
  file(
    GENERATE
    OUTPUT ${_control_file}
    CONTENT
      "# This file is generated content from add_postgresql_extension.
# No point in modifying it, it will be overwritten anyway.

# Default version, always set
default_version = '${_ext_VERSION}'

# Module pathname generated from target shared library name. Use
# MODULE_PATHNAME in script file.
module_pathname = '$libdir/$<TARGET_FILE_NAME:${NAME}>'

# Comment for extension. Set using COMMENT option. Can be set in
# script file as well.
$<$<NOT:$<BOOL:${_ext_COMMENT}>>:#>comment = '${_ext_COMMENT}'

# Encoding for script file. Set using ENCODING option.
$<$<NOT:$<BOOL:${_ext_ENCODING}>>:#>encoding = '${_ext_ENCODING}'

# Required extensions. Set using REQUIRES option (multi-valued).
$<$<NOT:$<BOOL:${_ext_REQUIRES}>>:#>requires = '$<JOIN:${_ext_REQUIRES},$<COMMA>>'
")

  install(TARGETS ${NAME} LIBRARY DESTINATION ${PostgreSQL_PACKAGE_LIBRARY_DIR})
  install(FILES ${_control_file} ${_script_files} DESTINATION ${PostgreSQL_EXTENSION_DIR})

  if(_ext_REGRESS)
    foreach(_test ${_ext_REGRESS})
      set(_sql_file "${CMAKE_CURRENT_SOURCE_DIR}/sql/${_test}.sql")
      set(_out_file "${CMAKE_CURRENT_SOURCE_DIR}/expected/${_test}.out")
      if(NOT EXISTS "${_sql_file}")
        message(FATAL_ERROR "Test file '${_sql_file}' does not exist!")
      endif()
      if(NOT EXISTS "${_out_file}")
        file(WRITE "${_out_file}" )
        message(STATUS "Created empty file ${_out_file}")
      endif()
    endforeach()

    if(PG_REGRESS)
      add_test(
        NAME ${NAME}
        COMMAND
          ${PG_REGRESS} --temp-instance=${CMAKE_BINARY_DIR}/tmp_check
          --inputdir=${CMAKE_CURRENT_SOURCE_DIR}
          --outputdir=${CMAKE_CURRENT_BINARY_DIR} --load-extension=${NAME}
          ${_ext_REGRESS})
    endif()

    add_custom_target(
      ${NAME}_update_results
      COMMAND
        ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_CURRENT_BINARY_DIR}/results/*.out
        ${CMAKE_CURRENT_SOURCE_DIR}/expected)
  endif()
endfunction()

if(PG_REGRESS)
  # We add a custom target to get output when there is a failure.
  add_custom_target(
    test_verbose COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
                         --verbose --output-on-failure)
endif()
