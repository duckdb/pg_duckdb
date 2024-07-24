cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME demo_in_pg)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

if (DEFINED ENV{PG_CONFIG})
    set(PGCONFIG "$ENV{PG_CONFIG}")
else ()
    find_program(PGCONFIG pg_config)
endif()

message(STATUS "Found pg_config: ${PGCONFIG}")

if (NOT PGCONFIG)
    message(FATAL_ERROR "Could not find pg_config")
endif ()

execute_process(COMMAND ${PGCONFIG} --version OUTPUT_VARIABLE PGVERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "PostgreSQL version: ${PGVERSION}")

execute_process(COMMAND ${PGCONFIG} --includedir --includedir-server OUTPUT_VARIABLE PostgreSQL_ACTUAL_INCLUDE_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)


project(${TARGET_NAME})
include_directories(src/include)
# include_directories(${PostgreSQL_ACTUAL_INCLUDE_DIR})
include_directories(/home/jelte/work/postgres-16/src/include)

set(EXTENSION_SOURCES
  src/demo_in_pg_extension.cpp
  # src/demo_tam.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

find_package(Intl)

if (Intl_FOUND)
    include_directories(${Intl_INCLUDE_DIRS})
endif ()


# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

# Define list of undefined symbols that are provided by the PostgreSQL server
set(POSTGRES_UNDEFINED_SYMBOLS
  "-Wl,-U,_PostmasterPid"
  "-Wl,-U,_IsUnderPostmaster"
  "-Wl,-U,_ExecutorRun_hook"
  "-Wl,-U,_standard_ExecutorRun"
  "-Wl,-U,_errfinish"
  "-Wl,-U,_errmsg_internal"
  "-Wl,-U,_errstart"
  "-Wl,-U,_emit_log_hook"
)

if(UNIX AND NOT APPLE)
    set(POSTGRES_UNDEFINED_SYMBOLS
        "-Wl,--unresolved-symbols,ignore-all"
    )
else()
    # Define list of undefined symbols that are provided by the PostgreSQL server
    set(POSTGRES_UNDEFINED_SYMBOLS
        "-Wl,-U,_PostmasterPid"
        "-Wl,-U,_IsUnderPostmaster"
        "-Wl,-U,_ExecutorRun_hook"
        "-Wl,-U,_standard_ExecutorRun"
        "-Wl,-U,_errfinish"
        "-Wl,-U,_errmsg_internal"
        "-Wl,-U,_errstart"
        "-Wl,-U,_emit_log_hook"
    )
endif()

# # "-Wl,-U,_PostmasterPid" "-Wl,-U,_IsUnderPostmaster"
target_link_options(${EXTENSION_NAME} PUBLIC ${POSTGRES_UNDEFINED_SYMBOLS}) #
target_link_options(${LOADABLE_EXTENSION_NAME} PUBLIC ${POSTGRES_UNDEFINED_SYMBOLS})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
