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

#include "quack/utility/copy.hpp"
#include "quack/scan/postgres_scan.hpp"
#include "quack/quack_duckdb.hpp"

static constexpr char quackCopyS3FilenamePrefix[] = "s3://";

static bool
create_relation_copy_parse_state(ParseState *pstate, const CopyStmt *stmt, List **vars, int stmt_location,
                                 int stmt_len) {
	ParseNamespaceItem *nsitem;
	RTEPermissionInfo *perminfo;
	TupleDesc tupDesc;
	List *attnums;
	ListCell *cur;
	Relation rel;
	Oid relid;

	/* Open and lock the relation, using the appropriate lock type. */
	rel = table_openrv(stmt->relation, AccessShareLock);

	relid = RelationGetRelid(rel);

	nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock, NULL, false, false);

	perminfo = nsitem->p_perminfo;
	perminfo->requiredPerms = ACL_SELECT;

	tupDesc = RelationGetDescr(rel);
	attnums = CopyGetAttnums(tupDesc, rel, stmt->attlist);

	foreach (cur, attnums) {
		int attno;
		Bitmapset **bms;
		attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;
		bms = &perminfo->selectedCols;
		*bms = bms_add_member(*bms, attno);
		*vars = lappend(*vars, makeVar(1, lfirst_int(cur), tupDesc->attrs[lfirst_int(cur) - 1].atttypid,
		                               tupDesc->attrs[lfirst_int(cur) - 1].atttypmod,
		                               tupDesc->attrs[lfirst_int(cur) - 1].attcollation, 0));
	}

	if (!ExecCheckPermissions(pstate->p_rtable, list_make1(perminfo), false)) {
		ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                  errmsg("(Quack) Failed Permission \"%s\"", RelationGetRelationName(rel))));
	}

	table_close(rel, AccessShareLock);

	/*
	 * RLS for relation. We should probably bail out at this point.
	 */
	if (check_enable_rls(relid, InvalidOid, false) == RLS_ENABLED) {
		ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                  errmsg("(Quack) RLS enabled on \"%s\"", RelationGetRelationName(rel))));
		return false;
	}

	return true;
}

bool
quack_copy(PlannedStmt *pstmt, const char *queryString, struct QueryEnvironment *queryEnv, uint64 *processed) {
	CopyStmt *copyStmt = (CopyStmt *)pstmt->utilityStmt;

	/* Copy `filename` should start with S3 prefix */
	if (duckdb::string(copyStmt->filename).rfind(quackCopyS3FilenamePrefix, 0)) {
		return false;
	}

	/* We handle only COPY .. TO */
	if (copyStmt->is_from) {
		return false;
	}

	List *rtables = NIL;
	List *vars = NIL;

	if (copyStmt->query) {
		List *rewritten;
		RawStmt *rawStmt;
		Query *query;

		rawStmt = makeNode(RawStmt);
		rawStmt->stmt = copyStmt->query;
		rawStmt->stmt_location = pstmt->stmt_location;
		rawStmt->stmt_len = pstmt->stmt_len;

		rewritten = pg_analyze_and_rewrite_fixedparams(rawStmt, queryString, NULL, 0, NULL);
		query = linitial_node(Query, rewritten);

		/* Extract required vars for table */
		int flags = PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS;
		vars = list_concat(pull_var_clause((Node *)query->targetList, flags),
		                   pull_var_clause((Node *)query->jointree->quals, flags));
	} else {
		ParseState *pstate = make_parsestate(NULL);
		pstate->p_sourcetext = queryString;
		pstate->p_queryEnv = queryEnv;
		if (!create_relation_copy_parse_state(pstate, copyStmt, &vars, pstmt->stmt_location, pstmt->stmt_len)) {
			return false;
		}
		rtables = pstate->p_rtable;
	}

	auto duckdbConnection = quack::quack_create_duckdb_connection(rtables, nullptr, vars, queryString);
	auto res = duckdbConnection->context->Query(queryString, false);

	if (res->HasError()) {
		elog(WARNING, "(Quack) %s", res->GetError().c_str());
		return false;
	}

	auto chunk = res->Fetch();
	*processed = chunk->GetValue(0, 0).GetValue<uint64_t>();
	return true;
}
