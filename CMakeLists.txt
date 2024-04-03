set(CMAKE_WARN_DEPRECATED
    OFF
    CACHE BOOL "" FORCE)
cmake_minimum_required(VERSION 3.2 FATAL_ERROR)

if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
  message(
    FATAL_ERROR
      "In-source builds not allowed.
    Please make a new directory and run CMake from there.
    You may need to remove CMakeCache.txt.")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Build Type - set a default build type `Release` if none was specified
set(PROJECT_DEFAULT_BUILD_TYPE "Release")

if(CMAKE_BUILD_TYPE AND NOT CMAKE_BUILD_TYPE MATCHES
                        "^(Debug|Release|RelWithDebInfo|MinSizeRel)$")
  message(FATAL_ERROR "Invalid value for CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}
    valid values are Debug|Release|RelWithDebInfo|MinSizeRel")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(
    STATUS
      "Setting build type to '${PROJECT_DEFAULT_BUILD_TYPE}' as none was specified."
  )
  set(CMAKE_BUILD_TYPE
      "${PROJECT_DEFAULT_BUILD_TYPE}"
      CACHE STRING "Choose the type of build." FORCE)
endif()

message(STATUS "CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE}")

string(TOLOWER "${PROJECT_NAME}" PROJECT_NAME_LOWER)

set(PROJECT_LIB_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}")
set(PROJECT_LIB_NAME "${PROJECT_NAME_LOWER}")

string(TIMESTAMP COMPILATION_DATE "%Y/%m/%d" UTC)

set(POSTGRESQL_MINIMUM_VERSION "16.0.0")
set(BOOST_MINIMUM_VERSION "1.81.0")

message(STATUS "POSTGRESQL_MINIMUM_VERSION=${POSTGRESQL_MINIMUM_VERSION}")
message(STATUS "BOOST_MINIMUM_VERSION=${BOOST_MINIMUM_VERSION}")

# PostgreSQL

find_package(PostgreSQL)

if(NOT PostgreSQL_FOUND OR NOT PostgreSQL_VERSION_STRING)
  message(
    FATAL_ERROR
      "PostgreSQL not found - Please check your PostgreSQL installation.")
endif()

# for XbetaY XalphaY XrcY -> X.Y
string(REGEX REPLACE "([0-9]+)[beta|alpha|rc|devel].*" "\\1.0"
                     POSTGRESQL_VERSION_STRING ${PostgreSQL_VERSION_STRING})
string(REGEX MATCH "([0-9]+)\.([0-9]+)" POSTGRESQL_VERSION
             "${PostgreSQL_VERSION_STRING}")

# for X.Y.Z -> XY  Y<10
string(REGEX REPLACE "^([0-9]+)\\.([0-9]+).*" "\\1\\2" PGSQL_VERSION
                     ${POSTGRESQL_VERSION})

if("${POSTGRESQL_VERSION}" VERSION_LESS "${POSTGRESQL_MINIMUM_VERSION}")
  message(
    FATAL_ERROR
      " PostgreSQL ${POSTGRESQL_MINIMUM_VERSION} or greater is required.")
endif("${POSTGRESQL_VERSION}" VERSION_LESS "${POSTGRESQL_MINIMUM_VERSION}")

include_directories(${PostgreSQL_SERVER_INCLUDE_DIRS})

# For Apple and Postgres 16 use .dylib instead of .so
if(APPLE AND POSTGRESQL_VERSION VERSION_GREATER_EQUAL "16")
  set(CMAKE_SHARED_MODULE_SUFFIX ".dylib")
endif()

# Boost

find_package(Boost ${BOOST_MINIMUM_VERSION} REQUIRED)

if(NOT Boost_VERSION_MACRO)
  set(Boost_VERSION_MACRO ${Boost_VERSION})
endif()

set(BOOST_VERSION
    "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")

include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

add_definitions(-DBOOST_ALLOW_DEPRECATED_HEADERS)

# Compiler

include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)
set(CMAKE_CXX_STANDARD 17)

set(COMPILER_VERSION "${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}")

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# compiler directives
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# https://www.postgresql.org/docs/10/xfunc-c.html

check_c_compiler_flag("-fPIC" C_COMPILER_SUPPORTS_FPIC)
check_cxx_compiler_flag("-fPIC" CXX_COMPILER_SUPPORTS_FPIC)

if(C_COMPILER_SUPPORTS_FPIC)
  set(CMAKE_C_FLAGS "-fPIC")
endif()
if(CXX_COMPILER_SUPPORTS_FPIC)
  set(CMAKE_CXX_FLAGS "-fPIC")
endif()

message(STATUS "COMPILER: ${CMAKE_CXX_COMPILER_ID}")

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  # Append CFLAGS/CXXFLAGS and PostgreSQL CFLAGS/CXXFAGS
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${PostgreSQL_CFLAGS} $ENV{CFLAGS}")
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -Wno-register ${PostgreSQL_CXXFLAGS} $ENV{CXXFLAGS}")

  # Debug compiler flags
  if(CMAKE_BUILD_TYPE MATCHES "Debug")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g3 -O0 -DDEBUG")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g3 -O0 -DDEBUG")
  endif()
  # Release compiler flags
  if(CMAKE_BUILD_TYPE MATCHES "Release")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
  endif()
endif()

message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

# Include directory

include_directories(${PROJECT_SOURCE_DIR}/include)

message(STATUS "PROJECT_LIB_NAME ${PROJECT_LIB_NAME}")

set(LINK_FLAGS "${PostgreSQL_SHARED_LINK_OPTIONS}")

foreach(_dir ${PostgreSQL_SERVER_LIBRARY_DIRS})
  set(LINK_FLAGS "${LINK_FLAGS} -L${_dir}")
endforeach()

if(APPLE)
  set(LINK_FLAGS
      "${LINK_FLAGS} -bundle_loader ${PG_BINARY} -undefined dynamic_lookup")
endif()
include(ExternalProject)
ExternalProject_Add(
  DUCKDB_LIB
  # PREFIX ${CMAKE_BINARY_DIR}/_deps
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/duckdb
  BINARY_DIR ${CMAKE_BINARY_DIR}/duckdb
  INSTALL_DIR ${CMAKE_BINARY_DIR}/duckdb
  # Build command
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_EXTENSIONS='parquet'
             -DASSERT_EXCEPTION=0 -DENABLE_SANITIZER=FALSE -DENABLE_UBSAN=0
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --config
                ${CMAKE_BUILD_TYPE}
  # Specify the build byproduct
  BUILD_BYPRODUCTS
    <BINARY_DIR>/src/libduckdb_static.a
    <BINARY_DIR>/extension/parquet/libparquet_extension.a
    <BINARY_DIR>/third_party/utf8proc/libduckdb_utf8proc.a
    <BINARY_DIR>/third_party/skiplist/libduckdb_skiplistlib.a
    <BINARY_DIR>/third_party/re2/libduckdb_re2.a
    <BINARY_DIR>/third_party/miniz/libduckdb_miniz.a
    <BINARY_DIR>/third_party/mbedtls/libduckdb_mbedtls.a
    <BINARY_DIR>/third_party/libpg_query/libduckdb_pg_query.a
    <BINARY_DIR>/third_party/imdb/libimdb.a
    <BINARY_DIR>/third_party/hyperloglog/libduckdb_hyperloglog.a
    <BINARY_DIR>/third_party/fsst/libduckdb_fsst.a
    <BINARY_DIR>/third_party/fmt/libduckdb_fmt.a
    <BINARY_DIR>/third_party/fastpforlib/libduckdb_fastpforlib.a
    # Optional: Pass additional CMake arguments if needed CMAKE_ARGS
    # -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR> ...
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON
  LOG_INSTALL ON # Optional: Enable logging for the install step
)

ExternalProject_Get_Property(DUCKDB_LIB install_dir)

# Add the library directory to the linker search path
link_directories(${install_dir}/src)

add_library(duckdb STATIC IMPORTED GLOBAL)
set_target_properties(duckdb PROPERTIES IMPORTED_LOCATION
                                        ${install_dir}/src/libduckdb_static.a)

add_library(duckdb_parquet STATIC IMPORTED GLOBAL)
set_target_properties(
  duckdb_parquet
  PROPERTIES IMPORTED_LOCATION
             ${install_dir}/extension/parquet/libparquet_extension.a)

include_directories(third_party/duckdb/src/include)
include_directories(include)
add_subdirectory(src)

set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES})

add_postgresql_extension(
  quack
  VERSION
  0.0.1
  SOURCES
  ${ALL_OBJECT_FILES}
  SCRIPTS
  sql/quack--0.0.1.sql)

add_dependencies(quack ${quack_LIBRARY} DUCKDB_LIB)
target_link_libraries(
  quack
  ${install_dir}/src/libduckdb_static.a
  ${install_dir}/third_party/utf8proc/libduckdb_utf8proc.a
  ${install_dir}/third_party/skiplist/libduckdb_skiplistlib.a
  ${install_dir}/third_party/re2/libduckdb_re2.a
  ${install_dir}/third_party/miniz/libduckdb_miniz.a
  ${install_dir}/third_party/mbedtls/libduckdb_mbedtls.a
  ${install_dir}/third_party/libpg_query/libduckdb_pg_query.a
  ${install_dir}/third_party/imdb/libimdb.a
  ${install_dir}/third_party/hyperloglog/libduckdb_hyperloglog.a
  ${install_dir}/third_party/fsst/libduckdb_fsst.a
  ${install_dir}/third_party/fmt/libduckdb_fmt.a
  ${install_dir}/third_party/fastpforlib/libduckdb_fastpforlib.a
  ${install_dir}/extension/parquet/libparquet_extension.a)

add_subdirectory("regress")

# Copy compile-commands.json to ROOT

if(CMAKE_EXPORT_COMPILE_COMMANDS)
  add_custom_target(
    copy-compile-commands ALL
    ${CMAKE_COMMAND} -E copy_if_different
    ${CMAKE_BINARY_DIR}/compile_commands.json ${PROJECT_SOURCE_DIR})
endif()
