#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "access/table.h"
#include "commands/copy.h"
#include "executor/executor.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "tcop/tcopprot.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
#include "pgduckdb/pgduckdb_ruleutils.h"
}

#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"

static constexpr char s3_filename_prefix[] = "s3://";
static constexpr char gcs_filename_prefix[] = "gs://";
static constexpr char r2_filename_prefix[] = "r2://";

/*
 * Returns the relation of the copy_stmt as a fully qualified DuckDB table reference. This is done
 * including the column names if provided in the original copy_stmt, e.g. my_table(column1, column2).
 * This also checks permissions on the table to see if the user is allowed to copy the data from this table.
 */
static duckdb::string
CreateRelationCopyString(ParseState *pstate, CopyStmt *copy_stmt, bool *allowed) {
	ParseNamespaceItem *nsitem;
#if PG_VERSION_NUM >= 160000
	RTEPermissionInfo *perminfo;
#else
	RangeTblEntry *rte;
#endif
	Relation rel;
	Oid relid;
	duckdb::string relation_copy;

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
	if (!ExecCheckPermissions(pstate->p_rtable, list_make1(perminfo), false)) {
		ereport(WARNING,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("(PGDuckDB/CreateRelationCopyString) Failed Permission \"%s\"", RelationGetRelationName(rel))));
		*allowed = false;
	}
#else
	if (!ExecCheckRTPerms(pstate->p_rtable, true)) {
		ereport(WARNING,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("(PGDuckDB/CreateRelationCopyString) Failed Permission \"%s\"", RelationGetRelationName(rel))));
		*allowed = false;
	}
#endif

	table_close(rel, AccessShareLock);

	if (!*allowed) {
		return relation_copy;
	}

	/*
	 * RLS for relation. We should probably bail out at this point.
	 */
	if (check_enable_rls(relid, InvalidOid, false) == RLS_ENABLED) {
		ereport(WARNING,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("(PGDuckDB/CreateRelationCopyString) RLS enabled on \"%s\"", RelationGetRelationName(rel))));
		*allowed = false;
		return relation_copy;
	}

	relation_copy += pgduckdb_relation_name(relid);
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

	*allowed = true;
	return relation_copy;
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

	duckdb::string query_string_copy = std::string(query_string);

	/*
	 * Quote filename so we can match it with query string to reverse find location of
	 * OPTION part of query.
	 * This is temporary solution, concrete solution should be construct OPTION part string from
	 * information provided in copy_stmt->options.
	 */
	auto filename_quoted = quote_literal_cstr(copy_stmt->filename);
	auto filename_quoted_end_pos = query_string_copy.rfind(filename_quoted) + strlen(filename_quoted);
	duckdb::string options_part  = query_string_copy.substr(filename_quoted_end_pos + 1);

	duckdb::string rewritten_query_string;

	if (copy_stmt->query) {
		List *rewritten;
		RawStmt *raw_stmt;
		Query *query;

		raw_stmt = makeNode(RawStmt);
		raw_stmt->stmt = copy_stmt->query;
		raw_stmt->stmt_location = pstmt->stmt_location;
		raw_stmt->stmt_len = pstmt->stmt_len;

		rewritten = pg_analyze_and_rewrite_fixedparams(raw_stmt, query_string, NULL, 0, NULL);
		query = linitial_node(Query, rewritten);
		rewritten_query_string = duckdb::StringUtil::Format("COPY (%s) TO %s %s", pgduckdb_pg_get_querydef(query, false),
		                                                   filename_quoted, options_part);
	} else {
		bool copy_allowed = true;
		ParseState *pstate = make_parsestate(NULL);
		pstate->p_sourcetext = query_string;
		pstate->p_queryEnv = query_env;
		duckdb::string relation_copy_part = CreateRelationCopyString(pstate, copy_stmt, &copy_allowed);
		if (!copy_allowed) {
			return false;
		}
		rewritten_query_string =
		    duckdb::StringUtil::Format("COPY %s TO %s %s", relation_copy_part, filename_quoted, options_part);
	}

	elog(DEBUG2, "(PGDuckDB/CreateRelationCopyString) Rewritten query: \'%s\'", rewritten_query_string.c_str());
	pfree(filename_quoted);

	auto duckdb_connection = pgduckdb::DuckDBManager::Get().GetConnection();
	auto res = duckdb_connection->context->Query(rewritten_query_string, false);

	if (res->HasError()) {
		elog(WARNING, "(PGDuckDB/DuckdbCopy) Execution failed with an error: %s", res->GetError().c_str());
		return false;
	}

	auto chunk = res->Fetch();
	*processed = chunk->GetValue(0, 0).GetValue<uint64_t>();
	return true;
}
