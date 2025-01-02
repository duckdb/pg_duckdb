duckdb_extension_load(json)
duckdb_extension_load(icu)
# This is relative to third_party/duckdb dir
duckdb_extension_load(cached_httpfs
    SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../cached_httpfs"
    INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../cached_httpfs/include"
)