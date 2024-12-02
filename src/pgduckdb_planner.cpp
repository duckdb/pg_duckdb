#include "pgduckdb/pgduckdb_planner.hpp"

#include "duckdb.hpp"

#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

extern "C" {
#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "optimizer/planmain.h"
#include "tcop/pquery.h"
#include "utils/syscache.h"
#include "utils/guc.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

bool duckdb_explain_analyze = false;

duckdb::unique_ptr<duckdb::PreparedStatement>
DuckdbPrepare(const Query *query) {
	Query *copied_query = (Query *)copyObjectImpl(query);
	const char *query_string = pgduckdb_get_querydef(copied_query);

	if (ActivePortal && ActivePortal->commandTag == CMDTAG_EXPLAIN) {
		if (duckdb_explain_analyze) {
			query_string = psprintf("EXPLAIN ANALYZE %s", query_string);
		} else {
			query_string = psprintf("EXPLAIN %s", query_string);
		}
	}

	elog(DEBUG2, "(PGDuckDB/DuckdbPrepare) Preparing: %s", query_string);

	auto con = pgduckdb::DuckDBManager::GetConnection();
	auto prepared_query = con->context->Prepare(query_string);
	return prepared_query;
}

static Plan *
CreatePlan(Query *query, bool throw_error) {
	int elevel = throw_error ? ERROR : WARNING;
	/*
	 * Prepare the query, se we can get the returned types and column names.
	 */

	duckdb::unique_ptr<duckdb::PreparedStatement> prepared_query = DuckdbPrepare(query);

	if (prepared_query->HasError()) {
		elog(elevel, "(PGDuckDB/CreatePlan) Prepared query returned an error: '%s", prepared_query->GetError().c_str());
		return nullptr;
	}

	CustomScan *duckdb_node = makeNode(CustomScan);

	auto &prepared_result_types = prepared_query->GetTypes();

	for (size_t i = 0; i < prepared_result_types.size(); i++) {
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

		TargetEntry *target_entry =
		    makeTargetEntry((Expr *)var, i + 1, (char *)pstrdup(prepared_query->GetNames()[i].c_str()), false);

		/* Our custom scan node needs the custom_scan_tlist to be set */
		duckdb_node->custom_scan_tlist = lappend(duckdb_node->custom_scan_tlist, copyObjectImpl(target_entry));

		/* But we also need an actual target list, because Postgres expects it
		 * for things like materialization */
		duckdb_node->scan.plan.targetlist = lappend(duckdb_node->scan.plan.targetlist, target_entry);

		ReleaseSysCache(tp);
	}

	duckdb_node->custom_private = list_make1(query);
	duckdb_node->methods = &duckdb_scan_scan_methods;

	return (Plan *)duckdb_node;
}

PlannedStmt *
DuckdbPlanNode(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params,
               bool throw_error) {
	/* We need to check can we DuckDB create plan */

	Plan *plan = InvokeCPPFunc(CreatePlan, parse, throw_error);
	Plan *duckdb_plan = (Plan *)castNode(CustomScan, plan);

	if (!duckdb_plan) {
		return nullptr;
	}

	/*
	 * If creating a plan for a scrollable cursor add a Material node at the
	 * top because or CustomScan does not support backwards scanning.
	 */
	if (cursor_options & CURSOR_OPT_SCROLL) {
		duckdb_plan = materialize_finished_plan(duckdb_plan);
	}

	/*
	 * We let postgres generate a basic plan, but then completely overwrite the
	 * actual plan with our CustomScan node. This is useful to get the correct
	 * values for all the other many fields of the PLannedStmt.
	 *
	 * XXX: The primary reason we do this is that Postgres fills in permInfos
	 * and rtable correctly. Those are needed for postgres to do its permission
	 * checks on the used tables.
	 *
	 * FIXME: For some reason this needs an additional query copy to allow
	 * re-planning of the query later during execution. But I don't really
	 * understand why this is needed.
	 */
	Query *copied_query = (Query *)copyObjectImpl(parse);
	PlannedStmt *postgres_plan = standard_planner(copied_query, query_string, cursor_options, bound_params);

	postgres_plan->planTree = duckdb_plan;

	return postgres_plan;
}
