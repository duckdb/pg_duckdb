#include "postgres.h"
#include "nodes/parsenodes.h"

char *pgduckdb_relation_name(Oid relid);
char *pgduckdb_function_name(Oid function_oid);
char *pgduckdb_get_querydef(Query *);
char *pgduckdb_get_tabledef(Oid relation_id);
List *pgduckdb_db_and_schema(const char *postgres_schema_name, bool is_duckdb_table);
const char *pgduckdb_db_and_schema_string(const char *postgres_schema_name, bool is_duckdb_table);
