#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "tcop/tcopprot.h"
#include "optimizer/optimizer.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/lsyscache.h"
}

#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/vendor/pg_list.hpp"

static constexpr char s3_filename_prefix[] = "s3://";
static constexpr char gcs_filename_prefix[] = "gs://";
static constexpr char r2_filename_prefix[] = "r2://";

static bool
CreateRelationCopyParseState(ParseState *pstate, const CopyStmt *stmt, List **vars, int stmt_location, int stmt_len) {
	ParseNamespaceItem *nsitem;
	RTEPermissionInfo *perminfo;
	TupleDesc tuple_desc;
	List *attnums;
	Relation rel;
	Oid relid;

	/* Open and lock the relation, using the appropriate lock type. */
	rel = table_openrv(stmt->relation, AccessShareLock);

	relid = RelationGetRelid(rel);

	nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock, NULL, false, false);

	perminfo = nsitem->p_perminfo;
	perminfo->requiredPerms = ACL_SELECT;

	tuple_desc = RelationGetDescr(rel);
	attnums = CopyGetAttnums(tuple_desc, rel, stmt->attlist);

	foreach_int(cur, attnums) {
		int attno;
		Bitmapset **bms;
		attno = cur - FirstLowInvalidHeapAttributeNumber;
		bms = &perminfo->selectedCols;
		*bms = bms_add_member(*bms, attno);
		*vars =
		    lappend(*vars, makeVar(1, cur, tuple_desc->attrs[cur - 1].atttypid, tuple_desc->attrs[cur - 1].atttypmod,
		                           tuple_desc->attrs[cur - 1].attcollation, 0));
	}

	if (!ExecCheckPermissions(pstate->p_rtable, list_make1(perminfo), false)) {
		ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                  errmsg("(Duckdb) Failed Permission \"%s\"", RelationGetRelationName(rel))));
	}

	table_close(rel, AccessShareLock);

	/*
	 * RLS for relation. We should probably bail out at this point.
	 */
	if (check_enable_rls(relid, InvalidOid, false) == RLS_ENABLED) {
		ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                  errmsg("(Duckdb) RLS enabled on \"%s\"", RelationGetRelationName(rel))));
		return false;
	}

	return true;
}

bool
DuckdbCopy(PlannedStmt *pstmt, const char *query_string, struct QueryEnvironment *query_env, uint64 *processed) {
	CopyStmt *copy_stmt = (CopyStmt *)pstmt->utilityStmt;

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

	List *rtables = NIL;
	List *vars = NIL;

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

		/* Extract required vars for table */
		int flags = PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS;
		vars = list_concat(pull_var_clause((Node *)query->targetList, flags),
		                   pull_var_clause((Node *)query->jointree->quals, flags));
	} else {
		ParseState *pstate = make_parsestate(NULL);
		pstate->p_sourcetext = query_string;
		pstate->p_queryEnv = query_env;
		if (!CreateRelationCopyParseState(pstate, copy_stmt, &vars, pstmt->stmt_location, pstmt->stmt_len)) {
			return false;
		}
		rtables = pstate->p_rtable;
	}

	auto duckdb_connection = pgduckdb::DuckdbCreateConnection(rtables, nullptr, vars, query_string);
	auto res = duckdb_connection->context->Query(query_string, false);

	if (res->HasError()) {
		elog(WARNING, "(PGDuckDB/DuckdbCopy) Execution failed with an error: %s", res->GetError().c_str());
		return false;
	}

	auto chunk = res->Fetch();
	*processed = chunk->GetValue(0, 0).GetValue<uint64_t>();
	return true;
}
