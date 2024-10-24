#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "access/table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "catalog/namespace.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/lsyscache.h"
#include "tcop/tcopprot.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

static constexpr char s3_filename_prefix[] = "s3://";
static constexpr char gcs_filename_prefix[] = "gs://";
static constexpr char r2_filename_prefix[] = "r2://";

/*
 * Returns the relation of the copy_stmt as a fully qualified DuckDB table reference. This is done
 * including the column names if provided in the original copy_stmt, e.g. my_table(column1, column2).
 */
static duckdb::string
CreateRelationCopyString(ParseState *pstate, CopyStmt *copy_stmt) {
	ParseNamespaceItem *nsitem;
#if PG_VERSION_NUM >= 160000
	RTEPermissionInfo *perminfo;
#else
	RangeTblEntry *rte;
#endif
	Relation rel;
	Oid relid;

	/* Open and lock the relation, using the appropriate lock type. */
	rel = table_openrv(copy_stmt->relation, AccessShareLock);
	relid = RelationGetRelid(rel);
	nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock, NULL, false, false);

#if PG_VERSION_NUM >= 160000
	perminfo = nsitem->p_perminfo;
	perminfo->requiredPerms = ACL_SELECT;
#else
	rte = nsitem->p_rte;
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

	std::string relation_copy = pgduckdb_relation_name(relid);
	if (copy_stmt->attlist) {
		ListCell *lc;
		relation_copy += "(";
		bool first = true;
		foreach (lc, copy_stmt->attlist) {
			if (!first) {
				relation_copy += ", ";
			}
			first = false;
			relation_copy += quote_identifier(strVal(lfirst(lc)));
		}
		relation_copy += ") ";
	}

	return relation_copy;
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

static duckdb::string
CreateCopyOptions(CopyStmt *copy_stmt, bool *options_valid) {
	duckdb::string options_string;
	duckdb::vector<duckdb::string> options_parts;

	if (list_length(copy_stmt->options) == 0) {
		return ";";
	}

	options_string = "(";

	bool first = true;
	foreach_node(DefElem, defel, copy_stmt->options) {
		if (!first) {
			options_string += ", ";
		}
		options_string += defel->defname;
		if (defel->arg) {
			options_string += " ";
			switch (nodeTag(defel->arg)) {
			case T_Integer:
			case T_Float:
			case T_BooleanTest:
				options_string += defGetString(defel);
				break;
			case T_String:
			case T_TypeName:
				options_string += quote_literal_cstr(defGetString(defel));
				break;
			case T_List:
				options_string += NameListToQuotedString((List *)defel->arg);
				break;
			case T_A_Star:
				options_string += "*";
				break;
			default:
				elog(ERROR, "unexpected node type in COPY: %d", (int)nodeTag(defel->arg));
			}
		}

		first = false;
	}
	options_string += ");";
	return options_string;
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
		ListCell *lc;

		/* examine queries to determine which error message to issue */
		foreach (lc, rewritten) {
			Query *q = lfirst_node(Query, lc);

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
DuckdbCopy(PlannedStmt *pstmt, const char *query_string, struct QueryEnvironment *query_env, uint64 *processed) {
	CopyStmt *copy_stmt = (CopyStmt *)pstmt->utilityStmt;

	if (!copy_stmt->filename) {
		return false;
	}

	/* Copy `filename` should start with S3/GS/R2 prefix */
	if (duckdb::string(copy_stmt->filename).rfind(s3_filename_prefix, 0) &&
	    duckdb::string(copy_stmt->filename).rfind(gcs_filename_prefix, 0) &&
	    duckdb::string(copy_stmt->filename).rfind(r2_filename_prefix, 0)) {
		return false;
	}

	/* We handle only COPY .. TO */
	if (copy_stmt->is_from) {
		return false;
	}

	bool options_valid = true;
	duckdb::string options_string = CreateCopyOptions(copy_stmt, &options_valid);
	if (!options_valid) {
		return false;
	}

	auto filename_quoted = quote_literal_cstr(copy_stmt->filename);

	duckdb::string rewritten_query_string;

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

		rewritten_query_string = duckdb::StringUtil::Format("COPY (%s) TO %s %s", pgduckdb_get_querydef(query),
		                                                    filename_quoted, options_string);
	} else {
		ParseState *pstate = make_parsestate(NULL);
		pstate->p_sourcetext = query_string;
		pstate->p_queryEnv = query_env;
		duckdb::string relation_copy_part = CreateRelationCopyString(pstate, copy_stmt);
		rewritten_query_string =
		    duckdb::StringUtil::Format("COPY %s TO %s %s", relation_copy_part, filename_quoted, options_string);
	}

	elog(DEBUG2, "(PGDuckDB/CreateRelationCopyString) Rewritten query: \'%s\'", rewritten_query_string.c_str());
	pfree(filename_quoted);

	auto res = pgduckdb::DuckDBQueryOrThrow(rewritten_query_string);
	auto chunk = res->Fetch();
	*processed = chunk->GetValue(0, 0).GetValue<uint64_t>();
	return true;
}
