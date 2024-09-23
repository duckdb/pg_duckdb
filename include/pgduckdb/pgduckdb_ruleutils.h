#include "postgres.h"

char *pgduckdb_relation_name(Oid relid);
char *pgduckdb_function_name(Oid function_oid);
char *pgduckdb_get_tabledef(Oid relation_id);
