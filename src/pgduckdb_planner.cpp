#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "optimizer/optimizer.h"
#include "tcop/pquery.h"
#include "utils/syscache.h"
#include "utils/guc.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
}

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

bool duckdb_explain_analyze = false;

std::tuple<duckdb::unique_ptr<duckdb::PreparedStatement>, duckdb::unique_ptr<duckdb::Connection>>
DuckdbPrepare(const Query *query) {
	/*
	 * For now, we don't support DuckDB queries in transactions. To support
	 * write queries in transactions we'll need to link Postgres and DuckdB
	 * their transaction lifecycles.
	 */
	PreventInTransactionBlock(true, "DuckDB queries");

	Query *copied_query = (Query *)copyObjectImpl(query);
	const char *query_string = pgduckdb_pg_get_querydef(copied_query, false);

	if (ActivePortal && ActivePortal->commandTag == CMDTAG_EXPLAIN) {
		if (duckdb_explain_analyze) {
			query_string = psprintf("EXPLAIN ANALYZE %s", query_string);
		} else {
			query_string = psprintf("EXPLAIN %s", query_string);
		}
	}

	elog(DEBUG2, "(PGDuckDB/DuckdbPrepare) Preparing: %s", query_string);

	auto duckdb_connection = pgduckdb::DuckDBManager::CreateConnection();
	auto prepared_query = duckdb_connection->context->Prepare(query_string);
	return {std::move(prepared_query), std::move(duckdb_connection)};
}

static Plan *
CreatePlan(Query *query, bool throw_error) {
	int elevel = throw_error ? ERROR : WARNING;
	/*
	 * Prepare the query, se we can get the returned types and column names.
	 */
	auto prepare_result = DuckdbPrepare(query);
	auto prepared_query = std::move(std::get<0>(prepare_result));

	if (prepared_query->HasError()) {
		elog(elevel, "(PGDuckDB/CreatePlan) Prepared query returned an error: '%s", prepared_query->GetError().c_str());
		return nullptr;
	}

	CustomScan *duckdb_node = makeNode(CustomScan);

	auto &prepared_result_types = prepared_query->GetTypes();

	for (auto i = 0; i < prepared_result_types.size(); i++) {
		auto &column = prepared_result_types[i];
		Oid postgresColumnOid = pgduckdb::GetPostgresDuckDBType(column);

		if (!OidIsValid(postgresColumnOid)) {
			elog(elevel, "(PGDuckDB/CreatePlan) Cache lookup failed for type %u", postgresColumnOid);
			return nullptr;
		}

		HeapTuple tp;
		Form_pg_type typtup;

		tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(postgresColumnOid));
		if (!HeapTupleIsValid(tp)) {
			elog(elevel, "(PGDuckDB/CreatePlan) Cache lookup failed for type %u", postgresColumnOid);
			return nullptr;
		}

		typtup = (Form_pg_type)GETSTRUCT(tp);

		Var *var = makeVar(INDEX_VAR, i + 1, postgresColumnOid, typtup->typtypmod, typtup->typcollation, 0);

		duckdb_node->custom_scan_tlist =
		    lappend(duckdb_node->custom_scan_tlist,
		            makeTargetEntry((Expr *)var, i + 1, (char *)pstrdup(prepared_query->GetNames()[i].c_str()), false));

		ReleaseSysCache(tp);
	}

	duckdb_node->custom_private = list_make1(query);
	duckdb_node->methods = &duckdb_scan_scan_methods;

	return (Plan *)duckdb_node;
}

PlannedStmt *
DuckdbPlanNode(Query *parse, int cursor_options, bool throw_error) {
	/* We need to check can we DuckDB create plan */
	Plan *plan = pgduckdb::DuckDBFunctionGuard<Plan *>(CreatePlan, "CreatePlan", parse, throw_error);
	Plan *duckdb_plan = (Plan *)castNode(CustomScan, plan);

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
#if PG_VERSION_NUM >= 160000
	result->permInfos = NULL;
#endif
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
