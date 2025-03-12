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
#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

bool duckdb_explain_analyze = false;
bool duckdb_explain_ctas = false;

duckdb::unique_ptr<duckdb::PreparedStatement>
DuckdbPrepare(const Query *query, bool allow_explain) {
	Query *copied_query = (Query *)copyObjectImpl(query);
	const char *query_string = pgduckdb_get_querydef(copied_query);

	if (allow_explain && ActivePortal && ActivePortal->commandTag == CMDTAG_EXPLAIN) {
		if (duckdb_explain_analyze) {
			if (duckdb_explain_ctas) {
				throw duckdb::NotImplementedException(
				    "Cannot use EXPLAIN ANALYZE with CREATE TABLE ... AS when using DuckDB execution");
			}

			query_string = psprintf("EXPLAIN ANALYZE %s", query_string);
		} else {
			query_string = psprintf("EXPLAIN %s", query_string);
		}
	}

	elog(DEBUG2, "(PGDuckDB/DuckdbPrepare) Preparing: %s", query_string);

	auto con = pgduckdb::DuckDBManager::GetConnection();
	return con->context->Prepare(query_string);
}

static Plan *
CreatePlan(Query *query, bool throw_error) {
	int elevel = throw_error ? ERROR : WARNING;
	/*
	 * Prepare the query, se we can get the returned types and column names.
	 */

	duckdb::unique_ptr<duckdb::PreparedStatement> prepared_query = DuckdbPrepare(query, false);

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

		/* We fill in the varno later, once we know the index of the custom RTE
		 * that we create. We'll know this at the end of DuckdbPlanNode. This
		 * can probably be simplified when we don't call the standard_planner
		 * anymore inside DuckdbPlanNode, because then we only need a single
		 * RTE. */
		Var *var = makeVar(0, i + 1, postgresColumnOid, typtup->typtypmod, typtup->typcollation, 0);

		TargetEntry *target_entry =
		    makeTargetEntry((Expr *)var, i + 1, (char *)pstrdup(prepared_query->GetNames()[i].c_str()), false);

		/* Our custom scan node needs the custom_scan_tlist to be set */
		duckdb_node->custom_scan_tlist = lappend(duckdb_node->custom_scan_tlist, copyObjectImpl(target_entry));

		/* For the plan its targetlist we use INDEX_VAR as the varno, which
		 * means it references our custom_scan_tlist. */
		var->varno = INDEX_VAR;

		/* But we also need an actual target list, because Postgres expects it
		 * for things like materialization */
		duckdb_node->scan.plan.targetlist = lappend(duckdb_node->scan.plan.targetlist, target_entry);

		ReleaseSysCache(tp);
	}

	duckdb_node->custom_private = list_make1(query);
	duckdb_node->methods = &duckdb_scan_scan_methods;

	return (Plan *)duckdb_node;
}

/* Creates a matching RangeTblEntry for the given CustomScan node */
static RangeTblEntry *
DuckdbRangeTableEntry(CustomScan *custom_scan) {
	List *column_names = NIL;
	foreach_node(TargetEntry, target_entry, custom_scan->scan.plan.targetlist) {
		column_names = lappend(column_names, makeString(target_entry->resname));
	}
	RangeTblEntry *rte = makeNode(RangeTblEntry);

	/* We need to choose an RTE kind here. RTE_RELATION does not work due to
	 * various asserts that fail due to us not setting some of the fields on
	 * the entry. Instead of filling those fields in with dummy values we use
	 * RTE_NAMEDTUPLESTORE, for which no special fields exist. */
	rte->rtekind = RTE_NAMEDTUPLESTORE;
	rte->eref = makeAlias("duckdb_scan", column_names);
	rte->inFromCl = true;

	return rte;
}

PlannedStmt *
DuckdbPlanNode(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params,
               bool throw_error) {
	/* We need to check can we DuckDB create plan */

	Plan *duckdb_plan = InvokeCPPFunc(CreatePlan, parse, throw_error);
	CustomScan *custom_scan = castNode(CustomScan, duckdb_plan);

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
	 * XXX: The primary reason we did this in the past is so that Postgres
	 * filled in permInfos and rtable correctly. Those are needed for postgres
	 * to do its permission checks on the used tables. We do these checks
	 * inside DuckDB as well, so that's not really necessary anymore. We still
	 * do this though to get all the other fields filled in correctly. Possibly
	 * we don't need to do this anymore.
	 *
	 * FIXME: For some reason this needs an additional query copy to allow
	 * re-planning of the query later during execution. But I don't really
	 * understand why this is needed.
	 */
	Query *copied_query = (Query *)copyObjectImpl(parse);
	PlannedStmt *postgres_plan = standard_planner(copied_query, query_string, cursor_options, bound_params);

	postgres_plan->planTree = duckdb_plan;

	/* Put a DuckdDB RTE at the end of the rtable */
	RangeTblEntry *rte = DuckdbRangeTableEntry(custom_scan);
	postgres_plan->rtable = lappend(postgres_plan->rtable, rte);

	/* Update the varno of the Var nodes in the custom_scan_tlist, to point to
	 * our new RTE. This should not be necessary anymore when we stop relying
	 * on the standard_planner here. */
	foreach_node(TargetEntry, target_entry, custom_scan->custom_scan_tlist) {
		Var *var = castNode(Var, target_entry->expr);

		var->varno = list_length(postgres_plan->rtable);
	}

	return postgres_plan;
}
