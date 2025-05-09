#include "pgduckdb/pgduckdb_planner.hpp"

#include "duckdb.hpp"

#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/pgduckdb_table_am.hpp"

extern "C" {
#include "postgres.h"

#include "access/xact.h"
#include "access/table.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "optimizer/planmain.h"
#include "parser/parse_coerce.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/pquery.h"
#include "utils/typcache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/guc.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

#include "pgduckdb/pg/types.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"
#include "pgduckdb/pgduckdb_guc.h"
#include "pgduckdb/pgduckdb_types.hpp"

static bool
ContainValueRTE(Query *query) {
	foreach_node(RangeTblEntry, rte, query->rtable) {
		if (rte->rtekind == RTE_VALUES) {
			return true;
		} else if (rte->rtekind == RTE_SUBQUERY) {
			if (ContainValueRTE(rte->subquery)) {
				return true;
			}
		}
	}
	return false;
}

bool
IsAllowedPostgresInsert(Query *query, bool throw_error) {
	if (query->commandType == CMD_SELECT) {
		return false;
	}

	int elevel = throw_error ? ERROR : DEBUG4;
	if (query->commandType != CMD_INSERT) {
		elog(duckdb_log_pg_explain ? NOTICE : elevel, "DuckDB only supports INSERT/SELECT on Postgres tables");
		return false;
	}

	Assert(list_length(query->rtable) >= query->resultRelation);
	RangeTblEntry *target_rel = (RangeTblEntry *)list_nth(query->rtable, query->resultRelation - 1);
	if (pgduckdb::IsDuckdbTable(target_rel->relid)) {
		return false;
	}

	/* Checking supported INSERT types */
	RangeTblEntry *select_rte = NULL;
	foreach_node(RangeTblEntry, rte, query->rtable) {
		if (rte->rtekind == RTE_SUBQUERY) {
			select_rte = rte;
		}
	}

	if (!select_rte) {
		elog(duckdb_log_pg_explain ? NOTICE : elevel, "DuckDB does not support INSERT without a subquery");
		return false;
	}

	/*
	 * The referenced rtables in the subquery should not include value RTEs. Literal input may vary between Postgres and
	 * DuckDB, such as differences in bytea representation and numeric rounding.
	 */
	if (ContainValueRTE(select_rte->subquery)) {
		elog(duckdb_log_pg_explain ? NOTICE : elevel, "DuckDB does not support INSERTs with value subqueries");
		return false;
	}

	Relation rel = RelationIdGetRelation(target_rel->relid);
	TupleDesc target_desc = RelationGetDescr(rel);
	bool ret = true;
	for (int i = 0; i < target_desc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(target_desc, i);
		if (attr->attisdropped) {
			continue;
		}

		/*
		 * Check if the target column type is supported by pg_duckdb. The type is allowed as long as the type conversion
		 * is implemented.
		 */
		auto duckdb_col_type = pgduckdb::ConvertPostgresToDuckColumnType(attr);
		if (duckdb_col_type.id() == duckdb::LogicalTypeId::USER) {
			elog(duckdb_log_pg_explain ? NOTICE : elevel,
			     "DuckDB does not support INSERTs into tables with column `%s` of unsupported type (OID %u). ",
			     NameStr(attr->attname), attr->atttypid);
			ret = false;
			break;
		}
	}
	RelationClose(rel);

	return ret;
}

duckdb::unique_ptr<duckdb::PreparedStatement>
DuckdbPrepare(const Query *query, const char *explain_prefix) {
	Query *copied_query = (Query *)copyObjectImpl(query);
	const char *query_string;
	if (IsAllowedPostgresInsert(copied_query)) {
		RangeTblEntry *select_rte = NULL;
		foreach_node(RangeTblEntry, rte, copied_query->rtable) {
			if (rte->rtekind == RTE_SUBQUERY) {
				select_rte = rte;
			}
		}

		/* A subquery must be present at this point; other cases should have been filtered out during the
		 * pre-planning phase */
		Assert(select_rte);
		query_string = pgduckdb_get_querydef(select_rte->subquery);
	} else {
		query_string = pgduckdb_get_querydef(copied_query);
	}

	if (explain_prefix) {
		query_string = psprintf("%s %s", explain_prefix, query_string);
	}

	elog(DEBUG2, "(PGDuckDB/DuckdbPrepare) Preparing: %s", query_string);

	auto con = pgduckdb::DuckDBManager::GetConnection();
	return con->context->Prepare(query_string);
}

/*
 * ReconstructTargetListForInsert - Aligns the target list with the table's columns
 *
 * When inserting data with a different column count or order than the target table,
 * this function reconstructs the target list to ensure proper alignment between
 * source and destination columns. It handles default values for unmatched columns.
 */
static List *
ReconstructTargetListForInsert(TupleDesc pg_tupdesc, List *query_targetlist, List *duckdb_targetlist) {
	List *target_list = NIL;
	ListCell *duckdb_targetlist_cell = list_head(duckdb_targetlist);

	for (int i = 0; i < pg_tupdesc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(pg_tupdesc, i);

		/* Skip dropped columns */
		if (attr->attisdropped)
			continue;

		/*
		 * Locate the matching column in the query target list.
		 * Use the existing TargetEntry if found, or create a NULL entry if not.
		 */
		TargetEntry *target_entry = NULL;
		for (int j = 0; j < list_length(query_targetlist); j++) {
			TargetEntry *query_target_entry = (TargetEntry *)list_nth(query_targetlist, j);
			if (query_target_entry->resno == attr->attnum) {
				if (pgduckdb_is_not_default_expr((Node *)query_target_entry, NULL)) {
					target_entry = (TargetEntry *)lfirst(duckdb_targetlist_cell);
					target_entry->resno = attr->attnum;
					duckdb_targetlist_cell = lnext(duckdb_targetlist, duckdb_targetlist_cell);
				} else {
					target_entry = query_target_entry;
				}
				break;
			}
		}

		if (target_entry) {
			target_list = lappend(target_list, target_entry);
			continue;
		}

		/* For column not found in the target list, create a NULL Expr for it */
		target_entry = makeTargetEntry((Expr *)makeNullConst(attr->atttypid, attr->atttypmod, attr->attcollation),
		                               attr->attnum, pstrdup(NameStr(attr->attname)), false);
		target_list = lappend(target_list, target_entry);
	}

	return target_list;
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
		Oid postgresColumnOid = pgduckdb::GetPostgresDuckDBType(prepared_result_types[i]);

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

	if (IsAllowedPostgresInsert(query)) {
		RangeTblEntry *target_rel = (RangeTblEntry *)list_nth(query->rtable, query->resultRelation - 1);
		Relation rel = RelationIdGetRelation(target_rel->relid);
		TupleDesc pg_tupdesc = RelationGetDescr(rel);

		/*
		 * When the target table's column count differs from the prepared result's column count (e.g., in an INSERT with
		 * explicit column names), we must reconstruct the target list to ensure proper column alignment between the
		 * source and destination.
		 */
		if (pg_tupdesc->natts != prepared_result_types.size()) {
			duckdb_node->scan.plan.targetlist =
			    ReconstructTargetListForInsert(pg_tupdesc, query->targetList, duckdb_node->scan.plan.targetlist);
		}

		RelationClose(rel);
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

/*
 * CoerceTargetList - Coerces the target list from DuckDB to match PostgreSQL types
 *
 * This function takes a target list from a DuckDB query and ensures that each column
 * has the correct data type expected by PostgreSQL. It adds type coercions where
 * necessary to make the types compatible.
 *
 * Parameters:
 *   duckdb_targetlist - The target list from the DuckDB query
 *   pg_tupdesc - PostgreSQL tuple descriptor containing the expected column types
 *
 * Returns:
 *   A new target list with appropriate type coercions applied
 */
List *
CoerceTargetList(List *duckdb_targetlist, TupleDesc pg_tupdesc) {
	List *ret = NIL;

	foreach_node(TargetEntry, source_te, duckdb_targetlist) {
		AttrNumber attnum = source_te->resno;
		if (attnum > pg_tupdesc->natts) {
			elog(ERROR, "DuckDB query returns more columns than Postgres wants");
		}

		/* Get expected target type */
		Form_pg_attribute attr = TupleDescAttr(pg_tupdesc, attnum - 1);
		Oid targetTypeId = attr->atttypid;
		int32 targetTypeMod = attr->atttypmod;

		/* Get source expression and its type */
		Expr *expr = source_te->expr;
		Oid sourceTypeId = exprType((Node *)expr);
		int32 sourceTypeMod = exprTypmod((Node *)expr);

		/* Add coercion if needed */
		if (sourceTypeId != targetTypeId || sourceTypeMod != targetTypeMod) {
			expr = (Expr *)coerce_to_target_type(NULL, (Node *)expr, sourceTypeId, targetTypeId, targetTypeMod,
			                                     COERCION_EXPLICIT, COERCE_IMPLICIT_CAST, -1);

			if (expr == NULL)
				elog(ERROR, "cannot coerce column %d from type %u to target type %u", attnum, sourceTypeId,
				     targetTypeId);

			/* Create a new target entry with coerced expression */
			TargetEntry *new_te = makeTargetEntry(expr, attnum, source_te->resname, source_te->resjunk);
			ret = lappend(ret, new_te);
		} else {
			ret = lappend(ret, source_te);
		}
	}

	return ret;
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
	 * values for all the other many fields of the PlannedStmt.
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
	if (IsAllowedPostgresInsert(parse)) {
		Assert(IsA(postgres_plan->planTree, ModifyTable));
		TupleDesc target_desc = ExecTypeFromTL(outerPlan(postgres_plan->planTree)->targetlist);
		duckdb_plan->targetlist = CoerceTargetList(duckdb_plan->targetlist, target_desc);
		outerPlan(postgres_plan->planTree) = duckdb_plan;
	} else {
		postgres_plan->planTree = duckdb_plan;
	}

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
