extern "C" {
#include "postgres.h"

#include "access/relation.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "storage/lockdefs.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
}

#include "pgduckdb/pgduckdb_table_am.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"

extern "C" {
char *
pgduckdb_function_name(Oid function_oid) {
	if (!pgduckdb::IsDuckdbOnlyFunction(function_oid)) {
		return nullptr;
	}
	auto func_name = get_func_name(function_oid);
	return psprintf("system.main.%s", quote_identifier(func_name));
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
	/*
	 * XXX: Should this be using get_namespace_name instead, the difference is
	 * that it will store temp tables in the pg_temp_123 style schemas instead
	 * of plain pg_temp. I don't think this really matters.
	 */
	const char *nspname = get_namespace_name_or_temp(relation->relnamespace);
	const char *dbname;

	if (relation->relam == pgduckdb::DuckdbTableAmOid()) {
		dbname = "memory";
	} else {
		dbname = "pgduckdb";
	}

	char *result = psprintf("%s.%s.%s", quote_identifier(dbname), quote_identifier(nspname), quote_identifier(relname));

	ReleaseSysCache(tp);

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
	const char *schema_name = get_namespace_name_or_temp(relation->rd_rel->relnamespace);

	StringInfoData buffer;
	initStringInfo(&buffer);

	if (get_rel_relkind(relation_oid) != RELKIND_RELATION) {
		elog(ERROR, "Only regular tables are supported in DuckDB");
	}

	appendStringInfo(&buffer, "CREATE SCHEMA IF NOT EXISTS %s; ", quote_identifier(schema_name));

	appendStringInfoString(&buffer, "CREATE ");

	if (relation->rd_rel->relpersistence != RELPERSISTENCE_TEMP) {
		elog(ERROR, "Only TEMP tables are supported in DuckDB");
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
}
