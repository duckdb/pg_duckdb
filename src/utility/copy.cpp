#include "pgduckdb/utility/copy.hpp"

#include <inttypes.h>

#include "pgduckdb/pgduckdb_guc.h"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_hooks.hpp"

extern "C" {
#include "postgres.h"

#include "access/table.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "common/string.h"
#include "executor/executor.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "storage/lockdefs.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"

#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/pgduckdb_ruleutils.h"
}

static constexpr char s3_filename_prefix[] = "s3://";
static constexpr char gcs_filename_prefix[] = "gs://";
static constexpr char r2_filename_prefix[] = "r2://";

/*
 * Returns the relation of the copy_stmt as a fully qualified DuckDB table reference. This is done
 * including the column names if provided in the original copy_stmt, e.g. my_table(column1, column2).
 */
static void
AppendCreateRelationCopyString(StringInfo info, ParseState *pstate, CopyStmt *copy_stmt) {
	/* Open and lock the relation, using the appropriate lock type. */
	Relation rel = table_openrv(copy_stmt->relation, AccessShareLock);
	Oid relid = RelationGetRelid(rel);
	ParseNamespaceItem *nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock, NULL, false, false);

#if PG_VERSION_NUM >= 160000
	RTEPermissionInfo *perminfo = nsitem->p_perminfo;
	perminfo->requiredPerms = ACL_SELECT;
#else
	RangeTblEntry *rte = nsitem->p_rte;
	rte->requiredPerms = ACL_SELECT;
#endif

#if PG_VERSION_NUM >= 160000
	ExecCheckPermissions(pstate->p_rtable, list_make1(perminfo), true);
#else
	ExecCheckRTPerms(pstate->p_rtable, true);
#endif

	table_close(rel, AccessShareLock);

	/*
	 * RLS for relation. We should probably bail out at this point.
	 */
	if (check_enable_rls(relid, InvalidOid, false) == RLS_ENABLED) {
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("(PGDuckDB/CreateRelationCopyString) RLS enabled on \"%s\", cannot use DuckDB based COPY",
		                RelationGetRelationName(rel))));
	}

	appendStringInfoString(info, pgduckdb_relation_name(relid));
	if (!copy_stmt->attlist) {
		return;
	}

	appendStringInfo(info, "(");
	bool first = true;

#if PG_VERSION_NUM >= 150000
	foreach_node(String, attr, copy_stmt->attlist) {
#else
	foreach_ptr(Value, attr, copy_stmt->attlist) {
#endif
		if (first) {
			first = false;
		} else {
			appendStringInfo(info, ", ");
		}

		appendStringInfoString(info, quote_identifier(strVal(attr)));
	}

	appendStringInfo(info, ") ");
}

/*
 * Checks if postgres permissions permit us to execute this query as the
 * current user.
 */
void
CheckQueryPermissions(Query *query, const char *query_string) {
	Query *copied_query = (Query *)copyObjectImpl(query);

	/* First we let postgres plan the query */
	PlannedStmt *postgres_plan = pg_plan_query(copied_query, query_string, CURSOR_OPT_PARALLEL_OK, NULL);

#if PG_VERSION_NUM >= 160000
	ExecCheckPermissions(postgres_plan->rtable, postgres_plan->permInfos, true);
#else
	ExecCheckRTPerms(postgres_plan->rtable, true);
#endif

	foreach_node(RangeTblEntry, rte, postgres_plan->rtable) {
		if (check_enable_rls(rte->relid, InvalidOid, false) == RLS_ENABLED) {
			ereport(ERROR,
			        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			         errmsg("(PGDuckDB/CheckQueryPermissions) RLS enabled on \"%s\", cannot use DuckDB based COPY",
			                get_rel_name(rte->relid))));
		}
	}
}

static char *
CommaSeparatedQuotedList(const List *names) {
	StringInfoData string;
	ListCell *l;

	initStringInfo(&string);

	foreach (l, names) {
		if (l != list_head(names))
			appendStringInfoChar(&string, ',');
		appendStringInfoString(&string, quote_identifier(strVal(lfirst(l))));
	}

	return string.data;
}

static void
AppendCreateCopyOptions(StringInfo info, CopyStmt *copy_stmt) {
	if (list_length(copy_stmt->options) == 0) {
		appendStringInfo(info, ";");
		return;
	}

	appendStringInfo(info, "(");

	bool first = true;
	foreach_node(DefElem, defel, copy_stmt->options) {
		if (first) {
			first = false;
		} else {
			appendStringInfo(info, ", ");
		}

		appendStringInfoString(info, defel->defname);
		if (defel->arg) {
			appendStringInfo(info, " ");
			switch (nodeTag(defel->arg)) {
			case T_Integer:
			case T_Float:
#if PG_VERSION_NUM >= 150000
			case T_Boolean:
#endif
				appendStringInfoString(info, defGetString(defel));
				break;
			case T_String:
			case T_TypeName:
				appendStringInfoString(info, quote_literal_cstr(defGetString(defel)));
				break;
			case T_List:
				appendStringInfo(info, "(");
				appendStringInfoString(info, CommaSeparatedQuotedList((List *)defel->arg));
				appendStringInfo(info, ")");
				break;
			case T_A_Star:
				appendStringInfo(info, "*");
				break;
			default:
				elog(ERROR, "Unexpected node type in COPY: %" PRIu64, (uint64_t)nodeTag(defel->arg));
			}
		}
	}

	appendStringInfo(info, ");");
}

/*
 * Throws an error if a rewritten raw statement returns an unexpected number of
 * queries (i.e. not just a single query). This is taken from Postgres its
 * BeginCopyTo function.
 */
static void
CheckRewritten(List *rewritten) {
	/* check that we got back something we can work with */
	if (rewritten == NIL) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("DO INSTEAD NOTHING rules are not supported for COPY")));
	} else if (list_length(rewritten) > 1) {
		/* examine queries to determine which error message to issue */
		foreach_node(Query, q, rewritten) {
			if (q->querySource == QSRC_QUAL_INSTEAD_RULE)
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				                errmsg("conditional DO INSTEAD rules are not supported for COPY")));
			if (q->querySource == QSRC_NON_INSTEAD_RULE)
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				                errmsg("DO ALSO rules are not supported for the COPY")));
		}

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("multi-statement DO INSTEAD rules are not supported for COPY")));
	}
}

static bool
CheckPrefix(const char *str, const char *prefix) {
	return strncmp(str, prefix, strlen(prefix)) == 0;
}

static bool
IsAllowedStatement(CopyStmt *stmt, bool throw_error = false) {
	int elevel = throw_error ? ERROR : DEBUG4;

	if (stmt->relation) {
		Relation rel = table_openrv(stmt->relation, AccessShareLock);
		bool is_duckdb_table = pgduckdb::IsDuckdbTable(rel);
		bool is_catalog_table = pgduckdb::IsCatalogTable(rel);
		table_close(rel, NoLock);
		if (is_catalog_table) {
			elog(elevel, "DuckDB does not support querying PG catalog tables");
			return false;
		}

		if (stmt->is_from && !is_duckdb_table) {
			elog(elevel, "pg_duckdb does not support COPY ... FROM ... yet for Postgres tables");
			return false;
		}
	}

	if (stmt->filename == NULL) {
		elog(elevel, "COPY ... TO STDOUT/FROM STDIN is not supported by DuckDB");
		return false;
	}

	return true;
}
static bool
ContainsDuckdbCopyOption(CopyStmt *stmt) {
	foreach_node(DefElem, defel, stmt->options) {
		if (strcmp(defel->defname, "format") == 0) {
			char *fmt = defGetString(defel);
			if (strcmp(fmt, "parquet") == 0) {
				return true;
			} else if (strcmp(fmt, "json") == 0) {
				return true;
			}
		} else if (strcmp(defel->defname, "partition_by") == 0 || strcmp(defel->defname, "use_tmp_file") == 0 ||
		           strcmp(defel->defname, "overwrite_or_ignore") == 0 || strcmp(defel->defname, "overwrite") == 0 ||
		           strcmp(defel->defname, "append") == 0 || strcmp(defel->defname, "filename_pattern") == 0 ||
		           strcmp(defel->defname, "file_extension") == 0 || strcmp(defel->defname, "per_thread_output") == 0 ||
		           strcmp(defel->defname, "file_size_bytes") == 0 ||
		           strcmp(defel->defname, "write_partition_columns") == 0 || strcmp(defel->defname, "array") == 0 ||
		           strcmp(defel->defname, "compression") == 0 || strcmp(defel->defname, "dateformat") == 0 ||
		           strcmp(defel->defname, "timestampformat") == 0 || strcmp(defel->defname, "nullstr") == 0 ||
		           strcmp(defel->defname, "prefix") == 0 || strcmp(defel->defname, "suffix") == 0 ||
		           strcmp(defel->defname, "compression_level") == 0 || strcmp(defel->defname, "field_ids") == 0 ||
		           strcmp(defel->defname, "row_group_size_bytes") == 0 ||
		           strcmp(defel->defname, "row_group_size") == 0 ||
		           strcmp(defel->defname, "row_groups_per_file") == 0) {
			return true;
		}
	}
	return false;
}

static bool
NeedsDuckdbExecution(CopyStmt *stmt) {
	/* Copy `filename` should start with S3/GS/R2 prefix */
	if (stmt->filename != NULL) {
		if (CheckPrefix(stmt->filename, s3_filename_prefix) || CheckPrefix(stmt->filename, gcs_filename_prefix) ||
		    CheckPrefix(stmt->filename, r2_filename_prefix)) {
			return true;
		}
		if (pg_str_endswith(stmt->filename, ".parquet") || pg_str_endswith(stmt->filename, ".json") ||
		    pg_str_endswith(stmt->filename, ".ndjson") || pg_str_endswith(stmt->filename, ".jsonl") ||
		    pg_str_endswith(stmt->filename, ".gz") || pg_str_endswith(stmt->filename, ".zst")) {
			return true;
		}
	}

	if (ContainsDuckdbCopyOption(stmt)) {
		return true;
	}

	if (!stmt->relation) {
		return false;
	}

	Relation rel = table_openrv(stmt->relation, AccessShareLock);
	bool is_duckdb_table = pgduckdb::IsDuckdbTable(rel);
	table_close(rel, NoLock);
	return is_duckdb_table;
}

const char *
MakeDuckdbCopyQuery(PlannedStmt *pstmt, const char *query_string, struct QueryEnvironment *query_env) {
	CopyStmt *copy_stmt = (CopyStmt *)pstmt->utilityStmt;

	if (NeedsDuckdbExecution(copy_stmt)) {
		IsAllowedStatement(copy_stmt, true);
	} else if (!duckdb_force_execution || !IsAllowedStatement(copy_stmt)) {
		return nullptr;
	}

	StringInfo rewritten_query_info = makeStringInfo();
	appendStringInfo(rewritten_query_info, "COPY ");
	if (copy_stmt->query) {
		RawStmt *raw_stmt = makeNode(RawStmt);
		raw_stmt->stmt = copy_stmt->query;
		raw_stmt->stmt_location = pstmt->stmt_location;
		raw_stmt->stmt_len = pstmt->stmt_len;

#if PG_VERSION_NUM >= 150000
		List *rewritten = pg_analyze_and_rewrite_fixedparams(raw_stmt, query_string, NULL, 0, NULL);
#else
		List *rewritten = pg_analyze_and_rewrite(raw_stmt, query_string, NULL, 0, NULL);
#endif
		CheckRewritten(rewritten);

		Query *query = linitial_node(Query, rewritten);
		CheckQueryPermissions(query, query_string);
		pgduckdb::IsAllowedStatement(query, true);

		appendStringInfo(rewritten_query_info, "(");
		appendStringInfoString(rewritten_query_info, pgduckdb_get_querydef(query));
		appendStringInfo(rewritten_query_info, ")");
	} else {
		ParseState *pstate = make_parsestate(NULL);
		pstate->p_sourcetext = query_string;
		pstate->p_queryEnv = query_env;
		AppendCreateRelationCopyString(rewritten_query_info, pstate, copy_stmt);
	}

	if (copy_stmt->is_from) {
		appendStringInfo(rewritten_query_info, " FROM ");
	} else {
		appendStringInfo(rewritten_query_info, " TO ");
	}
	appendStringInfoString(rewritten_query_info, quote_literal_cstr(copy_stmt->filename));
	appendStringInfo(rewritten_query_info, " ");
	AppendCreateCopyOptions(rewritten_query_info, copy_stmt);

	elog(DEBUG2, "(PGDuckDB/CreateRelationCopyString) Rewritten query: \'%s\'", rewritten_query_info->data);

	return rewritten_query_info->data;
}
