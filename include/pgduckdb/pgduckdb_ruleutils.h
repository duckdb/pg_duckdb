#include "postgres.h"
#include "pgduckdb/vendor/pg_list.hpp"

typedef struct StarReconstructionContext {
	List *target_list;
	int varno_star;
	int varattno_star;
	bool added_current_star;
} StarReconstructionContext;

char *pgduckdb_relation_name(Oid relid);
char *pgduckdb_function_name(Oid function_oid, bool *use_variadic_p);
char *pgduckdb_get_querydef(Query *);
char *pgduckdb_get_tabledef(Oid relation_id);
char *pgduckdb_get_alter_tabledef(Oid relation_oid, AlterTableStmt *alter_stmt);
char *pgduckdb_get_rename_tabledef(Oid relation_oid, RenameStmt *rename_stmt);
bool pgduckdb_is_not_default_expr(Node *node, void *context);
List *pgduckdb_db_and_schema(const char *postgres_schema_name, bool is_duckdb_table, bool is_ducklake_table);
const char *pgduckdb_db_and_schema_string(const char *postgres_schema_name, bool is_duckdb_table,
                                          bool is_ducklake_table);
bool pgduckdb_is_duckdb_row(Oid type_oid);
bool pgduckdb_is_unresolved_type(Oid type_oid);
bool pgduckdb_is_fake_type(Oid type_oid);
bool pgduckdb_var_is_duckdb_row(Var *var);
bool pgduckdb_func_returns_duckdb_row(RangeTblFunction *rtfunc);
Var *pgduckdb_duckdb_row_subscript_var(Expr *expr);
bool pgduckdb_reconstruct_star_step(StarReconstructionContext *ctx, ListCell *tle_cell);
int pgduckdb_show_type(Const *constval, int original_showtype);
bool pgduckdb_subscript_has_custom_alias(Plan *plan, List *rtable, Var *subscript_var, char *colname);
SubscriptingRef *pgduckdb_strip_first_subscript(SubscriptingRef *sbsref, StringInfo buf);
char *pgduckdb_write_row_refname(StringInfo buf, char *refname, bool is_top_level);
bool is_system_sampling(const char *tsm_name, int num_args);
bool is_bernoulli_sampling(const char *tsm_name, int num_args);
void pgduckdb_add_tablesample_percent(const char *tsm_name, StringInfo buf, int num_args);

extern bool outermost_query;
