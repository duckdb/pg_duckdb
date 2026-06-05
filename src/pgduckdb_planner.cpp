#include "pgduckdb/pgduckdb_planner.hpp"

#include "duckdb.hpp"

#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_planner.hpp"

extern "C" {
#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "optimizer/planmain.h"
#include "tcop/pquery.h"
#include "utils/syscache.h"
#include "utils/guc.h"
#include "parser/parse_relation.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pgduckdb/pgduckdb_ruleutils.h"

#if PG_VERSION_NUM >= 180000
#include "executor/executor.h"
#endif
}

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

duckdb::unique_ptr<duckdb::PreparedStatement>
DuckdbPrepare(const Query *query, const char *explain_prefix) {
	Query *copied_query = (Query *)copyObjectImpl(query);
	const char *query_string = pgduckdb_get_querydef(copied_query);

	if (explain_prefix) {
		query_string = psprintf("%s %s", explain_prefix, query_string);
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

	duckdb::unique_ptr<duckdb::PreparedStatement> prepared_query = DuckdbPrepare(query);

	if (prepared_query->HasError()) {
		elog(elevel, "(PGDuckDB/CreatePlan) Prepared query returned an error: %s", prepared_query->GetError().c_str());
		return nullptr;
	}

	CustomScan *duckdb_node = makeNode(CustomScan);

	auto &prepared_result_types = prepared_query->GetTypes();

	/*
	 * Postgres freezes the result column types of a query from the parser's
	 * point of view (PlanCacheComputeResultDesc -> ExecCleanTypeFromTL over the
	 * non-junk target list), and uses those types for the RowDescription in the
	 * extended query protocol / cached plans. DuckDB may type the same
	 * expression differently (e.g. ROUND(AVG(int) / c, n) is numeric in Postgres
	 * but double in DuckDB). If our plan declared the DuckDB type, the simple
	 * protocol would work but the extended protocol would emit the value under
	 * the parser's type, reinterpreting the bytes and corrupting the result.
	 *
	 * So to behave like Postgres in both protocols we declare the
	 * parser-assigned type for numeric columns -- the case where DuckDB and
	 * Postgres systematically disagree (DuckDB computes a double/decimal while
	 * Postgres' parser says numeric) -- and coerce DuckDB's value to numeric at
	 * execution time. Every other column keeps the DuckDB-derived type, which is
	 * what makes the simple protocol work today; the parser type for those is
	 * either identical or a placeholder (duckdb.row / unresolved_type) that only
	 * DuckDB can resolve. We also need the parser target list to line up 1:1
	 * with DuckDB's columns (it does not when e.g. a duckdb.row expands to "*").
	 */
	List *result_tlist = (query->commandType != CMD_SELECT && query->returningList != NIL) ? query->returningList
	                                                                                        : query->targetList;
	List *parser_tes = NIL;
	foreach_node(TargetEntry, tle, result_tlist) {
		if (!tle->resjunk) {
			parser_tes = lappend(parser_tes, tle);
		}
	}
	bool use_parser_types = static_cast<size_t>(list_length(parser_tes)) == prepared_result_types.size();

	/*
	 * The DuckDB-derived Postgres Oids, stashed in custom_private so the
	 * executor can detect a DuckDB result-type change between planning and
	 * execution (the plan's Var types are now the parser types, so we can no
	 * longer use them for that check).
	 */
	List *duckdb_column_oids = NIL;

	for (size_t i = 0; i < prepared_result_types.size(); i++) {
		Oid duckdb_column_oid = pgduckdb::GetPostgresDuckDBType(prepared_result_types[i], throw_error);

		if (!OidIsValid(duckdb_column_oid)) {
			return nullptr;
		}

		duckdb_column_oids = lappend_oid(duckdb_column_oids, duckdb_column_oid);

		/* Default to the DuckDB-derived type. */
		Oid column_oid = duckdb_column_oid;
		int32 column_typmod = pgduckdb::GetPostgresDuckDBTypemod(prepared_result_types[i]);
		Oid column_collation = get_typcollation(duckdb_column_oid);

		/*
		 * Override with the parser type only when Postgres' parser says the
		 * column is numeric but DuckDB produced a non-numeric type (i.e. a
		 * double/float). That is the case that corrupts results in the extended
		 * protocol, because the cached result descriptor (numeric) disagrees
		 * with the value we emit. When DuckDB already produces a numeric the oid
		 * matches, so we keep the DuckDB type: its typmod is harmless for the
		 * numeric wire format, and is the bounded DECIMAL that e.g. CREATE TABLE
		 * AS needs (Postgres' bare numeric is unbounded, which DuckDB can't
		 * store). We deliberately require an exact NUMERICOID match (not the base
		 * type) so domains over numeric keep the DuckDB type, since the value
		 * converter dispatches on the declared oid and does not handle domains.
		 */
		if (use_parser_types && duckdb_column_oid != NUMERICOID) {
			TargetEntry *tle = list_nth_node(TargetEntry, parser_tes, i);
			if (exprType((Node *)tle->expr) == NUMERICOID) {
				column_oid = NUMERICOID;
				column_typmod = exprTypmod((Node *)tle->expr);
				column_collation = exprCollation((Node *)tle->expr);
			}
		}

		/*
		 * We hardcode varno 1 here, because our final plan will only have a
		 * single RTE (this custom scan). In the past we put 0 here, and then
		 * filled it in later. If at some point we need multiple RTEs again, we
		 * might want to start doing that again.
		 */
		Var *var = makeVar(1, i + 1, column_oid, column_typmod, column_collation, 0);

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
	}

	duckdb_node->custom_private = list_make2(query, duckdb_column_oids);
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

static void
check_view_perms_recursive(Query *query) {
	ListCell *lc;

	if (query == NULL) {
		return;
	}

	foreach (lc, query->rtable) {
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

#if PG_VERSION_NUM < 160000
		if (rte->relkind == RELKIND_VIEW) {
			bool result = ExecCheckRTEPerms(rte);
			if (!result) {
				aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_VIEW, get_rel_name(rte->relid));
			}
		}
#else
		if (rte->perminfoindex != 0 && rte->relkind == RELKIND_VIEW) {
			RTEPermissionInfo *perminfo = getRTEPermissionInfo(query->rteperminfos, rte);
			bool result = ExecCheckOneRelPerms(perminfo);
			if (!result) {
				aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_VIEW, get_rel_name(perminfo->relid));
			}
		}
#endif

		if (rte->rtekind == RTE_SUBQUERY && rte->subquery) {
			check_view_perms_recursive(rte->subquery);
		}
	}

	if (query->cteList) {
		ListCell *lc_cte;
		foreach (lc_cte, query->cteList) {
			CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc_cte);
			if (IsA(cte->ctequery, Query)) {
				check_view_perms_recursive((Query *)cte->ctequery);
			}
		}
	}
}

PlannedStmt *
DuckdbPlanNode(Query *parse, int cursor_options, bool throw_error) {

	/* Properly check perms if there's a view or WITH statement */
	check_view_perms_recursive(parse);

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

	RangeTblEntry *rte = DuckdbRangeTableEntry(custom_scan);

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
	result->rtable = list_make1(rte);
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
