if(POLICY CMP0077)
    set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
endif()

#
# duckdb
#
execute_process(COMMAND git submodule update --init -- third_party/duckdb
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# Disable ASAN
SET(ENABLE_SANITIZER OFF)
SET(ENABLE_UBSAN OFF)

# No DuckDB cli
SET(BUILD_SHELL OFF)

# Disable unitest
SET(BUILD_UNITTESTS OFF)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/third_party/duckdb EXCLUDE_FROM_ALL)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/duckdb/src/include)

SET(QUACK_THIRD_PARTY_LIBS duckdb)