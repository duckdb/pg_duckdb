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
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/syscache.h"
#include "storage/lockdefs.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
#include "pgduckdb/pgduckdb_ruleutils.h"
#include "pgduckdb/vendor/pg_list.hpp"
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

/*
 * We never want to show some of our unresolved types in the DuckDB query.
 * These types only exist to make the Postgres parser and its type resolution
 * happy. DuckDB can simply figure out the correct type itself without an
 * explicit cast.
 */
bool
pgduckdb_is_fake_type(Oid type_oid) {
	if (pgduckdb_is_unresolved_type(type_oid)) {
		return true;
	}

	if (pgduckdb_is_duckdb_row(type_oid)) {
		return true;
	}

	if (pgduckdb::DuckdbJsonOid() == type_oid) {
		return true;
	}

	return false;
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
 * pgduckdb_check_for_star_start tries to figure out if this is tle_cell
 * contains a Var that is the start of a run of Vars that should be
 * reconstructed as a star. If that's the case it sets the varno_star and
 * varattno_star of the ctx.
 */
static void
pgduckdb_check_for_star_start(StarReconstructionContext *ctx, ListCell *tle_cell) {
	TargetEntry *first_tle = (TargetEntry *)lfirst(tle_cell);

	if (!IsA(first_tle->expr, Var)) {
		/* Not a Var so we're not at the start of a run of Vars. */
		return;
	}

	Var *first_var = (Var *)first_tle->expr;

	if (first_var->varattno != 1) {
		/* If we don't have varattno 1, then we are not at a run of Vars */
		return;
	}

	/*
	 * We found a Var that could potentially be the first of a run of Vars for
	 * which we have to reconstruct the star. To check if this is indeed the
	 * case we see if we can find a duckdb.row in this list of Vars.
	 */
	int varno = first_var->varno;
	int varattno = first_var->varattno;

	do {
		TargetEntry *tle = (TargetEntry *)lfirst(tle_cell);

		if (!IsA(tle->expr, Var)) {
			/*
			 * We found the end of this run of Vars, by finding something else
			 * than a Var.
			 */
			return;
		}

		Var *var = (Var *)tle->expr;

		if (var->varno != varno) {
			/* A Var from a different RTE */
			return;
		}

		if (var->varattno != varattno) {
			/* Not a consecutive Var */
			return;
		}
		if (pgduckdb_var_is_duckdb_row(var)) {
			/*
			 * If we have a duckdb.row, then we found a run of Vars that we
			 * have to reconstruct the star for.
			 */

			ctx->varno_star = varno;
			ctx->varattno_star = first_var->varattno;
			ctx->added_current_star = false;
			return;
		}

		/* Look for the next Var in the run */
		varattno++;
	} while ((tle_cell = lnext(ctx->target_list, tle_cell)));
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
 * This function returns true if we should skip writing this tle_cell to the
 * DuckDB query because it is part of a run of Vars that will be reconstructed
 * as a star.
 */
bool
pgduckdb_reconstruct_star_step(StarReconstructionContext *ctx, ListCell *tle_cell) {
	/* Detect start of a Var run that should be reconstructed to a star */
	pgduckdb_check_for_star_start(ctx, tle_cell);

	/*
	 * If we're not currently reconstructing a star we don't need to do
	 * anything.
	 */
	if (!ctx->varno_star) {
		return false;
	}

	TargetEntry *tle = (TargetEntry *)lfirst(tle_cell);

	/*
	 * Find out if this target entry is the next element in the run of Vars for
	 * the star we're currently reconstructing.
	 */
	if (tle->expr && IsA(tle->expr, Var)) {
		Var *var = castNode(Var, tle->expr);

		if (var->varno == ctx->varno_star && var->varattno == ctx->varattno_star) {
			/*
			 * We're still in the run of Vars, increment the varattno to look
			 * for the next Var on the next call.
			 */
			ctx->varattno_star++;

			/* If we already added star we skip writing this target entry */
			if (ctx->added_current_star) {
				return true;
			}

			/*
			 * If it's not a duckdb row we skip this target entry too. The way
			 * we add a single star is by expanding the first duckdb.row torget
			 * entry, which we've defined to expand to a star. So we need to
			 * skip any non duckdb.row Vars that precede the first duckdb.row.
			 */
			if (!pgduckdb_var_is_duckdb_row(var)) {
				return true;
			}

			ctx->added_current_star = true;
			return false;
		}
	}

	/*
	 * If it was not, that means we've successfully expanded this star and we
	 * should start looking for the next star start. So reset all the state
	 * used for this star reconstruction.
	 */
	ctx->varno_star = 0;
	ctx->varattno_star = 0;
	ctx->added_current_star = false;

	return false;
}

/*
 * iceberg_scan needs to be wrapped in an additianol subquery to resolve a
 * bug where aliasses on iceberg_scan are ignored:
 * https://github.com/duckdb/duckdb-iceberg/issues/44
 *
 * By wrapping the iceberg_scan call the alias is given to the subquery,
 * instead of th call. This subquery is easily optimized away by DuckDB,
 * because it doesn't do anything.
 *
 * This problem is also true for the "query" function, which we use when
 * creating materialized views and CTAS.
 * https://github.com/duckdb/duckdb/issues/15570#issuecomment-2598419474
 *
 * TODO: Probably check this in a bit more efficient way and move it to
 * pgduckdb_ruleutils.cpp
 */
bool
pgduckdb_function_needs_subquery(Oid function_oid) {
	if (!pgduckdb::IsDuckdbOnlyFunction(function_oid)) {
		return false;
	}

	auto func_name = get_func_name(function_oid);
	if (strcmp(func_name, "iceberg_scan") == 0) {
		return true;
	}

	if (strcmp(func_name, "query") == 0) {
		return true;
	}
	return false;
}

/*
 * A wrapper around pgduckdb_is_fake_type that returns -1 if the type of the
 * Const is fake, because that's the type of value that get_const_expr requires
 * in its showtype variable to never show the type.
 */
int
pgduckdb_show_type(Const *constval, int original_showtype) {
	if (pgduckdb_is_fake_type(constval->consttype)) {
		return -1;
	}
	return original_showtype;
}

bool
pgduckdb_subscript_has_custom_alias(Plan *plan, List *rtable, Var *subscript_var, char *colname) {
	/* The first bit of this logic is taken from get_variable() */
	int varno;
	int varattno;

	/*
	 * If we have a syntactic referent for the Var, and we're working from a
	 * parse tree, prefer to use the syntactic referent.  Otherwise, fall back
	 * on the semantic referent.  (See comments in get_variable().)
	 */
	if (subscript_var->varnosyn > 0 && plan == NULL) {
		varno = subscript_var->varnosyn;
		varattno = subscript_var->varattnosyn;
	} else {
		varno = subscript_var->varno;
		varattno = subscript_var->varattno;
	}

	RangeTblEntry *rte = rt_fetch(varno, rtable);

	/* Custom code starts here */
	char *original_column = strVal(list_nth(rte->eref->colnames, varattno - 1));

	return strcmp(original_column, colname) != 0;
}

/*
 * Subscript expressions that index into the duckdb.row type need to be changed
 * to regular column references in the DuckDB query. The main reason we do this
 * is so that DuckDB generates nicer column names, i.e. without the square
 * brackets: "mycolumn" instead of "r['mycolumn']"
 */
SubscriptingRef *
pgduckdb_strip_first_subscript(SubscriptingRef *sbsref, StringInfo buf) {
	if (!IsA(sbsref->refexpr, Var)) {
		return sbsref;
	}

	if (!pgduckdb_var_is_duckdb_row((Var *)sbsref->refexpr)) {
		return sbsref;
	}

	Assert(sbsref->refupperindexpr);
	Oid typoutput;
	bool typIsVarlena;
	Const *constval = castNode(Const, linitial(sbsref->refupperindexpr));
	getTypeOutputInfo(constval->consttype, &typoutput, &typIsVarlena);

	char *extval = OidOutputFunctionCall(typoutput, constval->constvalue);

	appendStringInfo(buf, ".%s", quote_identifier(extval));

	/*
	 * If there are any additional subscript expressions we should output them.
	 * Subscripts can be used in duckdb to index into arrays or json objects.
	 * It's fine if this results in an empty List, because printSubscripts
	 * handles that case correctly.
	 */
	SubscriptingRef *shorter_sbsref = (SubscriptingRef *)copyObjectImpl(sbsref);
	/* strip the first subscript from the list */
	shorter_sbsref->refupperindexpr = list_delete_first(shorter_sbsref->refupperindexpr);
	if (shorter_sbsref->reflowerindexpr) {
		shorter_sbsref->reflowerindexpr = list_delete_first(shorter_sbsref->reflowerindexpr);
	}
	return shorter_sbsref;
}

/*
 * Writes the refname to the buf in a way that results in the correct output
 * for the duckdb.row type.
 *
 * Returns the "attname" that should be passed back to the caller of
 * get_variable().
 */
char *
pgduckdb_write_row_refname(StringInfo buf, char *refname, bool is_top_level) {
	appendStringInfoString(buf, quote_identifier(refname));

	if (is_top_level) {
		/*
		 * If the duckdb.row is at the top level target list of a select, then
		 * we want to generate r.*, to unpack all the columns instead of
		 * returning a STRUCT from the query.
		 *
		 * Since we use .* there is no attname.
		 */
		appendStringInfoString(buf, ".*");
		return NULL;
	}

	/*
	 * In any other case, we want to simply use the alias of the TargetEntry.
	 */
	return refname;
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
