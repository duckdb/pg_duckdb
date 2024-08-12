cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME sync_catalogs_with_pg)

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
include_directories(${PostgreSQL_ACTUAL_INCLUDE_DIR})
include_directories(../../include)

set(EXTENSION_SOURCES
  src/sync_catalogs_with_pg_extension.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

find_package(Intl)

if (Intl_FOUND)
    include_directories(${Intl_INCLUDE_DIRS})
endif ()


# Define list of undefined symbols that are provided by the PostgreSQL server
if(UNIX AND NOT APPLE)
    set(POSTGRES_UNDEFINED_SYMBOLS
        "-Wl,--unresolved-symbols,ignore-all"
    )
else()
    # Define list of undefined symbols that are provided by the PostgreSQL server
    set(POSTGRES_UNDEFINED_SYMBOLS
        "-Wl,-U,_duckdb_default_db"
        "-Wl,-U,_emit_log_hook"
        "-Wl,-U,_errfinish"
        "-Wl,-U,_errmsg_internal"
        "-Wl,-U,_errstart_cold"
        "-Wl,-U,_errstart"
        "-Wl,-U,_ExecutorRun_hook"
        "-Wl,-U,_IsUnderPostmaster"
        "-Wl,-U,_pgduckdb_forward_create_table"
        "-Wl,-U,_PostmasterPid"
        "-Wl,-U,_SPI_connect"
        "-Wl,-U,_SPI_exec"
        "-Wl,-U,_SPI_finish"
        "-Wl,-U,_standard_ExecutorRun"
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
