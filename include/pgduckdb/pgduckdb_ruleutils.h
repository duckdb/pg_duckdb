#include "postgres.h"
#include "pgduckdb/vendor/pg_list.hpp"

typedef struct StarReconstructionContext {
	List *target_list;
	int varno_star;
	int varattno_star;
	bool added_current_star;
} StarReconstructionContext;

char *pgduckdb_relation_name(Oid relid);
char *pgduckdb_function_name(Oid function_oid);
char *pgduckdb_get_querydef(Query *);
char *pgduckdb_get_tabledef(Oid relation_id);
bool pgduckdb_is_not_default_expr(Node *node, void *context);
List *pgduckdb_db_and_schema(const char *postgres_schema_name, bool is_duckdb_table);
const char *pgduckdb_db_and_schema_string(const char *postgres_schema_name, bool is_duckdb_table);
bool pgduckdb_is_duckdb_row(Oid type_oid);
bool pgduckdb_is_unresolved_type(Oid type_oid);
bool pgduckdb_var_is_duckdb_row(Var *var);
bool pgduckdb_func_returns_duckdb_row(RangeTblFunction *rtfunc);
bool pgduckdb_target_list_contains_unresolved_type_or_row(List *target_list);
Var *pgduckdb_duckdb_row_subscript_var(Expr *expr);
bool pgduckdb_reconstruct_star_step(StarReconstructionContext *ctx, ListCell *tle_cell);
bool pgduckdb_function_needs_subquery(Oid function_oid);
int pgduckdb_show_type(Const *constval, int original_showtype);
bool pgduckdb_subscript_has_custom_alias(Plan *plan, List *rtable, Var *subscript_var, char *colname);
SubscriptingRef *pgduckdb_strip_first_subscript(SubscriptingRef *sbsref, StringInfo buf);
char *pgduckdb_write_row_refname(StringInfo buf, char *refname, bool is_top_level);

extern bool processed_targetlist;
