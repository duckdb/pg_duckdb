#include "pgduckdb/utility/copy.hpp"

#include <inttypes.h>

extern "C" {
#include "postgres.h"

#include "access/table.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
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
#include "pgduckdb/vendor/pg_list.hpp"
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

bool
CheckPrefix(const char *str, const char *prefix) {
	while (*prefix) {
		if (*prefix++ != *str++) {
			return false;
		}
	}
	return true;
}

const char *
MakeDuckdbCopyQuery(PlannedStmt *pstmt, const char *query_string, struct QueryEnvironment *query_env) {
	CopyStmt *copy_stmt = (CopyStmt *)pstmt->utilityStmt;
	if (!copy_stmt->filename) {
		return nullptr;
	}

	/* Copy `filename` should start with S3/GS/R2 prefix */
	if (!CheckPrefix(copy_stmt->filename, s3_filename_prefix) &&
	    !CheckPrefix(copy_stmt->filename, gcs_filename_prefix) &&
	    !CheckPrefix(copy_stmt->filename, r2_filename_prefix)) {
		return nullptr;
	}

	/* We handle only COPY .. TO */
	if (copy_stmt->is_from) {
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

		appendStringInfo(rewritten_query_info, "(");
		appendStringInfoString(rewritten_query_info, pgduckdb_get_querydef(query));
		appendStringInfo(rewritten_query_info, ")");
	} else {
		ParseState *pstate = make_parsestate(NULL);
		pstate->p_sourcetext = query_string;
		pstate->p_queryEnv = query_env;
		AppendCreateRelationCopyString(rewritten_query_info, pstate, copy_stmt);
	}

	appendStringInfo(rewritten_query_info, " TO ");
	appendStringInfoString(rewritten_query_info, quote_literal_cstr(copy_stmt->filename));
	appendStringInfo(rewritten_query_info, " ");
	AppendCreateCopyOptions(rewritten_query_info, copy_stmt);

	elog(DEBUG2, "(PGDuckDB/CreateRelationCopyString) Rewritten query: \'%s\'", rewritten_query_info->data);

	return rewritten_query_info->data;
}
