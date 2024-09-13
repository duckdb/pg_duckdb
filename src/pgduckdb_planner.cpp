#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "tcop/pquery.h"
#include "utils/syscache.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
}

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

bool duckdb_explain_analyze = false;

static PlannerInfo *
PlanQuery(Query *parse, ParamListInfo bound_params) {

	PlannerGlobal *glob = makeNode(PlannerGlobal);

	glob->boundParams = bound_params;
	glob->subplans = NIL;
	glob->subroots = NIL;
	glob->rewindPlanIDs = NULL;
	glob->finalrtable = NIL;
	glob->finalrteperminfos = NIL;
	glob->finalrowmarks = NIL;
	glob->resultRelations = NIL;
	glob->appendRelations = NIL;
	glob->relationOids = NIL;
	glob->invalItems = NIL;
	glob->paramExecTypes = NIL;
	glob->lastPHId = 0;
	glob->lastRowMarkId = 0;
	glob->lastPlanNodeId = 0;
	glob->transientPlan = false;
	glob->dependsOnRole = false;

	return subquery_planner(glob, parse, NULL, false, 0.0
#if PG_VERSION_NUM >= 170000
	                        ,
	                        NULL
#endif
	);
}

static Plan *
CreatePlan(Query *query, const char *query_string, ParamListInfo bound_params) {

	List *rtables = query->rtable;

	/* Extract required vars for table */
	int flags = PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS;
	List *vars = list_concat(pull_var_clause((Node *)query->targetList, flags),
	                         pull_var_clause((Node *)query->jointree->quals, flags));

	PlannerInfo *query_planner_info = PlanQuery(query, bound_params);
	auto duckdb_connection = pgduckdb::DuckdbCreateConnection(rtables, query_planner_info, vars, query_string);
	auto context = duckdb_connection->context;

	auto prepared_query = context->Prepare(query_string);

	if (prepared_query->HasError()) {
		elog(WARNING, "(PGDuckDB/CreatePlan) Prepared query returned an error: '%s",
		     prepared_query->GetError().c_str());
		return nullptr;
	}

	CustomScan *duckdb_node = makeNode(CustomScan);

	auto &prepared_result_types = prepared_query->GetTypes();

	for (auto i = 0; i < prepared_result_types.size(); i++) {
		auto &column = prepared_result_types[i];
		Oid postgresColumnOid = pgduckdb::GetPostgresDuckDBType(column);

		if (!OidIsValid(postgresColumnOid)) {
			elog(WARNING, "(PGDuckDB/CreatePlan) Cache lookup failed for type %u", postgresColumnOid);
			return nullptr;
		}

		HeapTuple tp;
		Form_pg_type typtup;

		tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(postgresColumnOid));
		if (!HeapTupleIsValid(tp)) {
			elog(WARNING, "(PGDuckDB/CreatePlan) Cache lookup failed for type %u", postgresColumnOid);
			return nullptr;
		}

		typtup = (Form_pg_type)GETSTRUCT(tp);

		Var *var = makeVar(INDEX_VAR, i + 1, postgresColumnOid, typtup->typtypmod, typtup->typcollation, 0);

		duckdb_node->custom_scan_tlist =
		    lappend(duckdb_node->custom_scan_tlist,
		            makeTargetEntry((Expr *)var, i + 1, (char *)prepared_query->GetNames()[i].c_str(), false));

		ReleaseSysCache(tp);
	}

	duckdb_node->custom_private = list_make2(duckdb_connection.release(), prepared_query.release());
	duckdb_node->methods = &duckdb_scan_scan_methods;

	return (Plan *)duckdb_node;
}

PlannedStmt *
DuckdbPlanNode(Query *parse, int cursor_options, ParamListInfo bound_params) {
	const char *query_string = pgduckdb_pg_get_querydef(parse, false);

	if (ActivePortal && ActivePortal->commandTag == CMDTAG_EXPLAIN) {
		if (duckdb_explain_analyze) {
			query_string = psprintf("EXPLAIN ANALYZE %s", query_string);
		} else {
			query_string = psprintf("EXPLAIN %s", query_string);
		}
	}
	/* We need to check can we DuckDB create plan */
	Plan *duckdb_plan = (Plan *)castNode(CustomScan, CreatePlan(parse, query_string, bound_params));

	if (!duckdb_plan) {
		return nullptr;
	}

	/* build the PlannedStmt result */
	PlannedStmt *result = makeNode(PlannedStmt);

	result->commandType = parse->commandType;
	result->queryId = parse->queryId;
	result->hasReturning = (parse->returningList != NIL);
	result->hasModifyingCTE = parse->hasModifyingCTE;
	result->canSetTag = parse->canSetTag;
	result->transientPlan = false;
	result->dependsOnRole = false;
	result->parallelModeNeeded = false;
	result->planTree = duckdb_plan;
	result->rtable = NULL;
	result->permInfos = NULL;
	result->resultRelations = NULL;
	result->appendRelations = NULL;
	result->subplans = NIL;
	result->rewindPlanIDs = NULL;
	result->rowMarks = NIL;
	result->relationOids = NIL;
	result->invalItems = NIL;
	result->paramExecTypes = NIL;

	/* utilityStmt should be null, but we might as well copy it */
	result->utilityStmt = parse->utilityStmt;
	result->stmt_location = parse->stmt_location;
	result->stmt_len = parse->stmt_len;

	return result;
}
