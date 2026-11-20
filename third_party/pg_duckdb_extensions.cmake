duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG f71cfdb4bff85b9e15b1f3b842d9aa395aff0167
)
# Static builds of pg_duckdb link httpfs in, the others load it as an extension
if(PG_DUCKDB_STATIC_HTTPFS)
    duckdb_extension_statically_link(httpfs)
endif()
