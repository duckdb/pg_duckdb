#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "utils/syscache.h"
}

#include "quack/quack_duckdb_connection.hpp"
#include "quack/quack_heap_scan.hpp"
#include "quack/quack_node.hpp"
#include "quack/quack_planner.hpp"
#include "quack/quack_types.hpp"
#include "quack/quack_utils.hpp"

static Plan *
quack_create_plan(Query *parse, const char *query) {
	/* Extract required vars for table */
	int flags = PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS;
	List *vars = list_concat(pull_var_clause((Node *)parse->targetList, flags),
	                         pull_var_clause((Node *)parse->jointree->quals, flags));

	auto duckdbConnection = quack::quack_create_duckdb_connection(parse->rtable, vars , query);
	auto context = duckdbConnection->context;

	auto preparedQuery = context->Prepare(query);

	if (preparedQuery->HasError()) {
		elog(WARNING, "(Quack) %s", preparedQuery->GetError().c_str());
		return nullptr;
	}

	CustomScan *quackNode = makeNode(CustomScan);

	auto &preparedResultTypes = preparedQuery->GetTypes();

	for (auto i = 0; i < preparedResultTypes.size(); i++) {
		auto &column = preparedResultTypes[i];
		Oid postgresColumnOid = quack::GetPostgresDuckDBType(column);

		if (!OidIsValid(postgresColumnOid)) {
			elog(ERROR, "Could not convert DuckDB to Postgres type, likely because the postgres->duckdb conversion was not supported");
		}
		HeapTuple tp;
		Form_pg_type typtup;

		tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(postgresColumnOid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for type %u", postgresColumnOid);

		typtup = (Form_pg_type)GETSTRUCT(tp);

		Var *var = makeVar(INDEX_VAR, i + 1, postgresColumnOid, typtup->typtypmod, typtup->typcollation, 0);

		quackNode->custom_scan_tlist =
			lappend(quackNode->custom_scan_tlist,
					makeTargetEntry((Expr *)var, i + 1, (char *)preparedQuery->GetNames()[i].c_str(), false));

		ReleaseSysCache(tp);
	}

	quackNode->custom_private = list_make2(duckdbConnection.release(), preparedQuery.release());
	quackNode->methods = &quack_scan_scan_methods;

	return (Plan *)quackNode;
}

PlannedStmt *
quack_plan_node(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams) {
	/* We need to check can we DuckDB create plan */
	Plan *quackPlan = (Plan *)castNode(CustomScan, quack_create_plan(parse, query_string));

	if (!quackPlan) {
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
	result->planTree = quackPlan;
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
