#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "access/relation.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "commands/dbcommands.h"
#include "nodes/nodeFuncs.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/syscache.h"
#include "storage/lockdefs.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_table_am.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"

extern "C" {
bool processed_targetlist = false;

char *
pgduckdb_function_name(Oid function_oid) {
	if (!pgduckdb::IsDuckdbOnlyFunction(function_oid)) {
		return nullptr;
	}

	auto func_name = get_func_name(function_oid);
	return psprintf("system.main.%s", quote_identifier(func_name));
}

bool
pgduckdb_is_unresolved_type(Oid type_oid) {
	return type_oid == pgduckdb::DuckdbUnresolvedTypeOid();
}

bool
pgduckdb_is_duckdb_row(Oid type_oid) {
	return type_oid == pgduckdb::DuckdbRowOid();
}

bool
pgduckdb_var_is_duckdb_row(Var *var) {
	if (!var) {
		return false;
	}
	return pgduckdb_is_duckdb_row(var->vartype);
}

bool
pgduckdb_func_returns_duckdb_row(RangeTblFunction *rtfunc) {
	if (!rtfunc) {
		return false;
	}

	if (!IsA(rtfunc->funcexpr, FuncExpr)) {
		return false;
	}

	FuncExpr *func_expr = castNode(FuncExpr, rtfunc->funcexpr);

	return pgduckdb_is_duckdb_row(func_expr->funcresulttype);
}

bool
pgduckdb_target_list_contains_unresolved_type_or_row(List *target_list) {
	foreach_node(TargetEntry, tle, target_list) {
		Oid type = exprType((Node *)tle->expr);
		if (pgduckdb_is_unresolved_type(type)) {
			return true;
		}

		if (pgduckdb_is_duckdb_row(type)) {
			return true;
		}
	}
	return false;
}

/*
 * Returns NULL if the expression is not a subscript on a duckdb row. Returns
 * the Var of the duckdb row if it is.
 */
Var *
pgduckdb_duckdb_row_subscript_var(Expr *expr) {
	if (!expr) {
		return NULL;
	}

	if (!IsA(expr, SubscriptingRef)) {
		return NULL;
	}

	SubscriptingRef *subscript = (SubscriptingRef *)expr;

	if (!IsA(subscript->refexpr, Var)) {
		return NULL;
	}

	Var *refexpr = (Var *)subscript->refexpr;

	if (!pgduckdb_var_is_duckdb_row(refexpr)) {
		return NULL;
	}
	return refexpr;
}

/*
 * In our DuckDB queries we sometimes want to use "SELECT *", when selecting
 * from a function like read_parquet. That way DuckDB can figure out the actual
 * columns that it should return. Sadly Postgres expands the * character from
 * the original query to a list of columns. So we need to put a star, any time
 * we want to replace duckdb.row columns with a "*" in the duckdb query.
 *
 * Since the original "*" might expand to many columns we need to remove all of
 * those, when putting a "*" back. To do so we try to find a runs of Vars from
 * the same FROM entry, aka RangeTableEntry (RTE) that we expect were created
 * with a *.
 *
 * This function tries to find the indexes of the first column for each of
 * those runs. It does so using this heuristic:
 *
 * 1. Find a column with varattno == 1 (aka the first column from an RTE)
 * 2. Find a consecutive run of columns from the same RTE with varattnos that
 *    keep increasing by 1.
 * 3. Once we find a duckdb.row column in any of those consecutive columns, we
 *    assume this run was created using a star expression and we track the
 *    initial index. Then we start at 1 again to find the next run.
 *
 * NOTE: This function does not find the end of such runs, that's left as an
 * excersice for the caller. It should be pretty easy for the caller to do
 * that, because they need to remove such columns anyway. The main reason this
 * function existis is so that the caller doesn't have to scan backwards to
 * find the start of a run once it finds a duckdb.row column. Scanning
 * backwards is difficult for the caller because it wants to write out columns
 * to the DuckDB query as it consumes them.
 */
List *
pgduckdb_star_start_vars(List *target_list) {
	List *star_start_indexes = NIL;
	Var *possible_star_start_var = NULL;
	int possible_star_start_var_index = 0;

	int i = 0;

	foreach_node(TargetEntry, tle, target_list) {
		i++;

		if (!IsA(tle->expr, Var)) {
			possible_star_start_var = NULL;
			possible_star_start_var_index = 0;
			continue;
		}

		Var *var = (Var *)tle->expr;

		if (var->varattno == 1) {
			possible_star_start_var = var;
			possible_star_start_var_index = i;
		} else if (possible_star_start_var) {
			if (var->varno != possible_star_start_var->varno || var->varno == i - possible_star_start_var_index + 1) {
				possible_star_start_var = NULL;
				possible_star_start_var_index = 0;
			}
		}

		if (pgduckdb_var_is_duckdb_row(var)) {
			star_start_indexes = lappend_int(star_start_indexes, possible_star_start_var_index);
			possible_star_start_var = NULL;
			possible_star_start_var_index = 0;
		}
	}
	return star_start_indexes;
}

/*
 * Given a postgres schema name, this returns a list of two elements: the first
 * is the DuckDB database name and the second is the duckdb schema name. These
 * are not escaped yet.
 */
List *
pgduckdb_db_and_schema(const char *postgres_schema_name, bool is_duckdb_table) {
	if (!is_duckdb_table) {
		return list_make2((void *)"pgduckdb", (void *)postgres_schema_name);
	}

	if (strcmp("pg_temp", postgres_schema_name) == 0) {
		return list_make2((void *)"pg_temp", (void *)"main");
	}

	if (strcmp("public", postgres_schema_name) == 0) {
		/* Use the "main" schema in DuckDB for tables in the public schema in Postgres */
		auto dbname = pgduckdb::DuckDBManager::Get().GetDefaultDBName().c_str();
		return list_make2((void *)dbname, (void *)"main");
	}

	if (strncmp("ddb$", postgres_schema_name, 4) != 0) {
		auto dbname = pgduckdb::DuckDBManager::Get().GetDefaultDBName().c_str();
		return list_make2((void *)dbname, (void *)postgres_schema_name);
	}

	StringInfoData db_name;
	StringInfoData schema_name;
	initStringInfo(&db_name);
	initStringInfo(&schema_name);
	const char *saveptr = &postgres_schema_name[4];
	const char *dollar;

	while ((dollar = strchr(saveptr, '$'))) {
		appendBinaryStringInfo(&db_name, saveptr, dollar - saveptr);
		saveptr = dollar + 1;
		if (saveptr[0] == '\0') {
			elog(ERROR, "Schema name is invalid");
		}

		if (saveptr[0] == '$') {
			appendStringInfoChar(&db_name, '$');
		} else {
			break;
		}
	}

	if (!dollar) {
		appendStringInfoString(&db_name, saveptr);
		return list_make2((void *)db_name.data, (char *)"main");
	}

	while ((dollar = strchr(saveptr, '$'))) {
		appendBinaryStringInfo(&schema_name, saveptr, dollar - saveptr);
		saveptr = dollar + 1;

		if (saveptr[0] == '$') {
			appendStringInfoChar(&schema_name, '$');
		} else {
			break;
		}
	}
	appendStringInfoString(&schema_name, saveptr);

	return list_make2(db_name.data, schema_name.data);
}

/*
 * Returns the fully qualified DuckDB database and schema. The schema and
 * database are quoted if necessary.
 */
const char *
pgduckdb_db_and_schema_string(const char *postgres_schema_name, bool is_duckdb_table) {
	List *db_and_schema = pgduckdb_db_and_schema(postgres_schema_name, is_duckdb_table);
	const char *db_name = (const char *)linitial(db_and_schema);
	const char *schema_name = (const char *)lsecond(db_and_schema);
	return psprintf("%s.%s", quote_identifier(db_name), quote_identifier(schema_name));
}

/*
 * generate_relation_name computes the fully qualified name of the relation in
 * DuckDB for the specified Postgres OID. This includes the DuckDB database name
 * too.
 */
char *
pgduckdb_relation_name(Oid relation_oid) {
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relation_oid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relation_oid);
	Form_pg_class relation = (Form_pg_class)GETSTRUCT(tp);
	const char *relname = NameStr(relation->relname);
	const char *postgres_schema_name = get_namespace_name_or_temp(relation->relnamespace);
	bool is_duckdb_table = pgduckdb::IsDuckdbTable(relation);

	if (!is_duckdb_table) {
		/*
		 * FIXME: This should be moved somewhere else. We already have a list
		 * of RTEs somwhere that we use to call ExecCheckPermissions. We could
		 * used that same list to check if RLS is enabled on any of the tables,
		 * instead of checking it here for **every occurence** of each table in
		 * the query. One benefit of having it here is that it ensures that we
		 * never forget to check for RLS.
		 *
		 * NOTE: We only need to check this for non-DuckDB tables because DuckDB
		 * tables don't support RLS anyway.
		 */
		if (check_enable_rls(relation_oid, InvalidOid, false) == RLS_ENABLED) {
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			                errmsg("(PGDuckDB/pgduckdb_relation_name) Cannot use \"%s\" in a DuckDB query, because RLS "
			                       "is enabled on it",
			                       get_rel_name(relation_oid))));
		}
	}

	const char *db_and_schema = pgduckdb_db_and_schema_string(postgres_schema_name, is_duckdb_table);

	char *result = psprintf("%s.%s", db_and_schema, quote_identifier(relname));

	ReleaseSysCache(tp);

	return result;
}

/*
 * pgduckdb_get_querydef returns the definition of a given query in DuckDB
 * syntax. This definition includes the query's SQL string, but does not
 * include the query's parameters.
 *
 * It's a small wrapper around pgduckdb_pg_get_querydef_internal to ensure that
 * dates are always formatted in ISO format (which is the only format that
 * DuckDB understands). The reason this is not part of
 * pgduckdb_pg_get_querydef_internal is because we want to avoid changing that
 * vendored in function as much as possible to keep updates easy.
 *
 * Apart from that it also sets the processed_targetlist variable to false,
 * which we use in get_target_list to determine if we're processing the
 * outermost targetlist or not.
 */
char *
pgduckdb_get_querydef(Query *query) {
	processed_targetlist = false;
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("DateStyle", "ISO, YMD", PGC_USERSET, PGC_S_SESSION);
	char *result = pgduckdb_pg_get_querydef_internal(query, false);
	AtEOXact_GUC(false, save_nestlevel);
	return result;
}

/*
 * pgduckdb_get_tabledef returns the definition of a given table. This
 * definition includes table's schema, default column values, not null and check
 * constraints. The definition does not include constraints that trigger index
 * creations; specifically, unique and primary key constraints are excluded.
 *
 * TODO: Add support indexes, primary keys and unique constraints.
 *
 * This function is inspired by the pg_get_tableschemadef_string function in
 * the following patch that I (Jelte) submitted to Postgres in 2023:
 * https://www.postgresql.org/message-id/CAGECzQSqdDHO_s8=CPTb2+4eCLGUscdh=KjYGTunhvrwcC7ZSQ@mail.gmail.com
 */
char *
pgduckdb_get_tabledef(Oid relation_oid) {
	Relation relation = relation_open(relation_oid, AccessShareLock);
	const char *relation_name = pgduckdb_relation_name(relation_oid);
	const char *postgres_schema_name = get_namespace_name_or_temp(relation->rd_rel->relnamespace);
	const char *db_and_schema = pgduckdb_db_and_schema_string(postgres_schema_name, pgduckdb::IsDuckdbTable(relation));

	StringInfoData buffer;
	initStringInfo(&buffer);

	if (get_rel_relkind(relation_oid) != RELKIND_RELATION) {
		elog(ERROR, "Only regular tables are supported in DuckDB");
	}

	appendStringInfo(&buffer, "CREATE SCHEMA IF NOT EXISTS %s; ", db_and_schema);

	appendStringInfoString(&buffer, "CREATE ");

	if (relation->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
		// allowed
	} else if (!pgduckdb::IsMotherDuckEnabledAnywhere()) {
		elog(ERROR, "Only TEMP tables are supported in DuckDB if MotherDuck support is not enabled");
	} else if (relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT) {
		elog(ERROR, "Only TEMP and non-UNLOGGED tables are supported in DuckDB");
	} else if (!pgduckdb::IsMotherDuckPostgresDatabase()) {
		elog(ERROR, "MotherDuck tables must be created in the duckb.motherduck_postgres_database");
	} else if (relation->rd_rel->relowner != pgduckdb::MotherDuckPostgresUser()) {
		elog(ERROR, "MotherDuck tables must be created by the duckb.motherduck_postgres_user");
	}

	appendStringInfo(&buffer, "TABLE %s (", relation_name);

	if (list_length(RelationGetFKeyList(relation)) > 0) {
		elog(ERROR, "DuckDB tables do not support foreign keys");
	}

	List *relation_context = pgduckdb_deparse_context_for(relation_name, relation_oid);

	/*
	 * Iterate over the table's columns. If a particular column is not dropped
	 * and is not inherited from another table, print the column's name and
	 * its formatted type.
	 */
	TupleDesc tuple_descriptor = RelationGetDescr(relation);
	TupleConstr *tuple_constraints = tuple_descriptor->constr;
	AttrDefault *default_value_list = tuple_constraints ? tuple_constraints->defval : NULL;

	bool first_column_printed = false;
	AttrNumber default_value_index = 0;
	for (int i = 0; i < tuple_descriptor->natts; i++) {
		Form_pg_attribute column = TupleDescAttr(tuple_descriptor, i);

		if (column->attisdropped) {
			continue;
		}

		const char *column_name = NameStr(column->attname);

		const char *column_type_name = format_type_with_typemod(column->atttypid, column->atttypmod);

		if (first_column_printed) {
			appendStringInfoString(&buffer, ", ");
		}
		first_column_printed = true;

		appendStringInfo(&buffer, "%s ", quote_identifier(column_name));
		appendStringInfoString(&buffer, column_type_name);

		if (column->attcompression) {
			elog(ERROR, "Column compression is not supported in DuckDB");
		}

		if (column->attidentity) {
			elog(ERROR, "Identity columns are not supported in DuckDB");
		}

		/* if this column has a default value, append the default value */
		if (column->atthasdef) {
			Assert(tuple_constraints != NULL);
			Assert(default_value_list != NULL);

			AttrDefault *default_value = &(default_value_list[default_value_index]);
			default_value_index++;

			Assert(default_value->adnum == (i + 1));
			Assert(default_value_index <= tuple_constraints->num_defval);

			/*
			 * convert expression to node tree, and prepare deparse
			 * context
			 */
			Node *default_node = (Node *)stringToNode(default_value->adbin);

			/* deparse default value string */
			char *default_string = pgduckdb_deparse_expression(default_node, relation_context, false, false);

			/*
			 * DuckDB does not support STORED generated columns, it does
			 * support VIRTUAL generated columns though. Howevever, Postgres
			 * currently does not support those, so for now there's no overlap
			 * in generated column support between the two databases.
			 */
			if (!column->attgenerated) {
				appendStringInfo(&buffer, " DEFAULT %s", default_string);
			} else if (column->attgenerated == ATTRIBUTE_GENERATED_STORED) {
				elog(ERROR, "DuckDB does not support STORED generated columns");
			} else {
				elog(ERROR, "Unkown generated column type");
			}
		}

		/* if this column has a not null constraint, append the constraint */
		if (column->attnotnull) {
			appendStringInfoString(&buffer, " NOT NULL");
		}

		/*
		 * XXX: default collation is actually probably not supported by
		 * DuckDB, unless it's C or POSIX. But failing unless people
		 * provide C or POSIX seems pretty annoying. How should we handle
		 * this?
		 */
		Oid collation = column->attcollation;
		if (collation != InvalidOid && collation != DEFAULT_COLLATION_OID && collation != C_COLLATION_OID &&
		    collation != POSIX_COLLATION_OID) {
			elog(ERROR, "DuckDB does not support column collations");
		}
	}

	/*
	 * Now check if the table has any constraints. If it does, set the number
	 * of check constraints here. Then iterate over all check constraints and
	 * print them.
	 */
	AttrNumber constraint_count = tuple_constraints ? tuple_constraints->num_check : 0;
	ConstrCheck *check_constraint_list = tuple_constraints ? tuple_constraints->check : NULL;

	for (AttrNumber i = 0; i < constraint_count; i++) {
		ConstrCheck *check_constraint = &(check_constraint_list[i]);

		/* convert expression to node tree, and prepare deparse context */
		Node *check_node = (Node *)stringToNode(check_constraint->ccbin);

		/* deparse check constraint string */
		char *check_string = pgduckdb_deparse_expression(check_node, relation_context, false, false);

		/* if an attribute or constraint has been printed, format properly */
		if (first_column_printed || i > 0) {
			appendStringInfoString(&buffer, ", ");
		}

		appendStringInfo(&buffer, "CONSTRAINT %s CHECK ", quote_identifier(check_constraint->ccname));

		appendStringInfoString(&buffer, "(");
		appendStringInfoString(&buffer, check_string);
		appendStringInfoString(&buffer, ")");
	}

	/* close create table's outer parentheses */
	appendStringInfoString(&buffer, ")");

	if (!pgduckdb::IsDuckdbTableAm(relation->rd_tableam)) {
		/* Shouldn't happen but seems good to check anyway */
		elog(ERROR, "Only a table with the DuckDB can be stored in DuckDB, %d %d", relation->rd_rel->relam,
		     pgduckdb::DuckdbTableAmOid());
	}

	if (relation->rd_options) {
		elog(ERROR, "Storage options are not supported in DuckDB");
	}

	relation_close(relation, AccessShareLock);

	return (buffer.data);
}

/*
 * Recursively check Const nodes and Var nodes for handling more complex DEFAULT clauses
 */
bool
pgduckdb_is_not_default_expr(Node *node, void *context) {
	if (node == NULL) {
		return false;
	}

	if (IsA(node, Var)) {
		return true;
	} else if (IsA(node, Const)) {
		/* If location is -1, it comes from the DEFAULT clause */
		Const *con = (Const *)node;
		if (con->location != -1) {
			return true;
		}
	}

#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, pgduckdb_is_not_default_expr, context);
#else
	return expression_tree_walker(node, (bool (*)())((void *)pgduckdb_is_not_default_expr), context);
#endif
}
}
