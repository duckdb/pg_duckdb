duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 77392a72077a85897db2519fae11e2c0da972b8a
)
# Static builds of pg_duckdb link httpfs in, the others load it as an extension
if(PG_DUCKDB_STATIC_HTTPFS)
    duckdb_extension_statically_link(httpfs)
endif()
