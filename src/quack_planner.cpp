#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/pg_type.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "utils/syscache.h"
}

#include "quack/quack_heap_scan.hpp"
#include "quack/quack_node.hpp"
#include "quack/quack_planner.hpp"
#include "quack/quack_types.hpp"
#include "quack/quack_utils.hpp"

namespace quack {

static duckdb::unique_ptr<duckdb::DuckDB>
quack_open_database() {
	duckdb::DBConfig config;
	// config.SetOption("memory_limit", "2GB");
	// config.SetOption("threads", "8");
	// config.allocator = duckdb::make_uniq<duckdb::Allocator>(QuackAllocate, QuackFree, QuackReallocate, nullptr);
	return duckdb::make_uniq<duckdb::DuckDB>(nullptr, &config);
}

} // namespace quack

static Plan *
quack_create_plan(Query *parse, const char *query) {
	auto db = quack::quack_open_database();

	/* Add heap tables */
	db->instance->config.replacement_scans.emplace_back(
	    quack::PostgresHeapReplacementScan,
	    duckdb::make_uniq_base<duckdb::ReplacementScanData, quack::PostgresHeapReplacementScanData>(parse, query));
	auto connection = duckdb::make_uniq<duckdb::Connection>(*db);

	// Add the postgres_scan inserted by the replacement scan
	auto &context = *connection->context;
	quack::PostgresHeapScanFunction heap_scan_fun;
	duckdb::CreateTableFunctionInfo heap_scan_info(heap_scan_fun);

	auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
	context.transaction.BeginTransaction();
	catalog.CreateTableFunction(context, &heap_scan_info);
	context.transaction.Commit();

	if (strlen(quack_secret) != 0) {
		std::vector<std::string> quackSecret = quack::tokenizeString(quack_secret, '#');
		StringInfo s3SecretKey = makeStringInfo();
		appendStringInfoString(s3SecretKey, "CREATE SECRET s3Secret ");
		appendStringInfo(s3SecretKey, "(TYPE S3, KEY_ID '%s', SECRET '%s', REGION '%s');", quackSecret[1].c_str(),
		                 quackSecret[2].c_str(), quackSecret[3].c_str());
		context.Query(s3SecretKey->data, false);
		pfree(s3SecretKey->data);
	}

	auto preparedQuery = context.Prepare(query);

	if (preparedQuery->HasError()) {
		elog(INFO, "(Quack) %s", preparedQuery->GetError().c_str());
		return nullptr;
	}

	CustomScan *quackNode = makeNode(CustomScan);

	auto &preparedResultTypes = preparedQuery->GetTypes();

	for (auto i = 0; i < preparedResultTypes.size(); i++) {
		auto &column = preparedResultTypes[i];
		Oid postgresColumnOid = quack::GetPostgresDuckDBType(column);

		if (OidIsValid(postgresColumnOid)) {
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
	}

	quackNode->custom_private = list_make2(db.release(), preparedQuery.release());
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
