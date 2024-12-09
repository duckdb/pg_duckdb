#include "postgres.h"

char *pgduckdb_relation_name(Oid relid);
char *pgduckdb_function_name(Oid function_oid);
char *pgduckdb_get_querydef(Query *);
char *pgduckdb_get_tabledef(Oid relation_id);
bool pgduckdb_is_not_default_expr(Node *node, void *context);
List *pgduckdb_db_and_schema(const char *postgres_schema_name, bool is_duckdb_table);
const char *pgduckdb_db_and_schema_string(const char *postgres_schema_name, bool is_duckdb_table);
bool pgduckdb_var_is_duckdb_row(Var *var);
bool pgduckdb_func_returns_duckdb_row(RangeTblFunction *rtfunc);
