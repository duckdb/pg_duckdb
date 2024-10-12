#include "postgres.h"

char *pgduckdb_relation_name(Oid relid);
char *pgduckdb_function_name(Oid function_oid);
char *pgduckdb_get_tabledef(Oid relation_id);
const char *pgduckdb_db_and_schema(const char *postgres_schema_name, bool is_duckdb_table);
