#include "pgduckdb/pgduckdb_planner.hpp"

#include "duckdb.hpp"

#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/pgduckdb_table_am.hpp"

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
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
#include "rewrite/rewriteManip.h"
#include "tcop/pquery.h"
#include "utils/typcache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/guc.h"
#include "parser/parse_relation.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
#include "utils/builtins.h"

#if PG_VERSION_NUM >= 180000
#include "executor/executor.h"
#endif
}

#include "pgduckdb/pg/types.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_node.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"
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

/*
 * Returns the range table index of the SELECT subquery of an INSERT, or 0 if
 * there is none. The parser wraps the source of an INSERT ... SELECT in a
 * single top-level subquery RTE, so there can be at most one, which the
 * Assert double-checks.
 */
static int
FindInsertSelectRTI(const Query *query) {
	int select_rti = 0;
	int rti = 1;
	foreach_node(RangeTblEntry, rte, query->rtable) {
		if (rte->rtekind == RTE_SUBQUERY) {
			Assert(select_rti == 0);
			select_rti = rti;
		}
		rti++;
	}
	return select_rti;
}

bool
IsAllowedPostgresInsert(Query *query, bool throw_error) {
	if (query->commandType == CMD_SELECT || query->resultRelation == 0) {
		return false;
	}

	Assert(list_length(query->rtable) >= query->resultRelation);
	RangeTblEntry *target_rel = (RangeTblEntry *)list_nth(query->rtable, query->resultRelation - 1);
	if (pgduckdb::IsDuckdbTable(target_rel->relid)) {
		return false;
	}

	int elevel = throw_error ? ERROR : DEBUG4;
	if (query->commandType != CMD_INSERT) {
		elog(elevel, "DuckDB only supports INSERT/SELECT on Postgres tables");
		return false;
	}

	/*
	 * EXPLAIN ANALYZE of an INSERT is supposed to actually insert the rows,
	 * but in our plan the DuckDB scan would run the SELECT inside a
	 * DuckDB-side EXPLAIN ANALYZE and return no rows to the ModifyTable node.
	 * Instead of silently inserting nothing we don't allow DuckDB execution
	 * for these statements. The duckdb_explain_analyze global is only valid
	 * while we're actually planning an EXPLAIN query, which is why we also
	 * check the commandTag of the ActivePortal.
	 */
	if (duckdb_explain_analyze && ActivePortal && ActivePortal->commandTag == CMDTAG_EXPLAIN) {
		elog(elevel, "DuckDB does not support EXPLAIN ANALYZE on INSERTs into Postgres tables");
		return false;
	}

	/* Checking supported INSERT types */
	int select_rti = FindInsertSelectRTI(query);
	if (select_rti == 0) {
		elog(elevel, "DuckDB does not support INSERT without a subquery");
		return false;
	}
	RangeTblEntry *select_rte = list_nth_node(RangeTblEntry, query->rtable, select_rti - 1);

	/*
	 * If the subquery references value RTEs we prefer executing the statement
	 * with Postgres, because literal input may vary between Postgres and
	 * DuckDB, such as differences in bytea representation and numeric
	 * rounding. But when DuckDB execution is required (which is the case
	 * whenever throw_error is set, e.g. because the query reads from
	 * `read_csv(...)`), falling back to Postgres is not an option, so then we
	 * accept those potential differences.
	 */
	if (!throw_error && ContainValueRTE(select_rte->subquery)) {
		elog(elevel, "DuckDB does not support INSERTs with value subqueries");
		return false;
	}

	Relation rel = RelationIdGetRelation(target_rel->relid);
	TupleDesc target_desc = RelationGetDescr(rel);
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
		if (duckdb_col_type.id() == duckdb::LogicalTypeId::INVALID) {
			elog(elevel, "DuckDB does not support INSERTs into tables with column `%s` of unsupported type (OID %u). ",
			     NameStr(attr->attname), attr->atttypid);
			RelationClose(rel);
			return false;
		}
	}
	RelationClose(rel);

	return true;
}

duckdb::unique_ptr<duckdb::PreparedStatement>
DuckdbPrepare(const Query *query, const char *explain_prefix) {
	Query *copied_query = (Query *)copyObjectImpl(query);
	const char *query_string;
	/*
	 * When we get here the decision to use DuckDB has already been made, so
	 * we pass throw_error=true. This matters for INSERTs with a VALUES RTE in
	 * their subquery, which are only allowed when DuckDB execution is
	 * required.
	 */
	if (IsAllowedPostgresInsert(copied_query, true)) {
		int select_rti = FindInsertSelectRTI(copied_query);

		/* A subquery must be present at this point; other cases should have been filtered out during the
		 * pre-planning phase */
		Assert(select_rti != 0);
		RangeTblEntry *select_rte = list_nth_node(RangeTblEntry, copied_query->rtable, select_rti - 1);
		Query *select_query = select_rte->subquery;

		/*
		 * CTEs attached to the INSERT statement itself would be lost when
		 * only the SELECT is deparsed, so we move them into the SELECT query.
		 * Because they move down a query level, all references to them from
		 * within the SELECT need their levelsup decremented, similar to what
		 * pull_up_simple_subquery does. That has to happen before attaching
		 * the moved CTEs, so that references between those CTEs themselves
		 * are not changed. The SELECT can also have CTEs of its own, in which
		 * case both lists are simply combined into one WITH clause. That
		 * needs no further fixups, because CTE references are resolved by
		 * name (the ctename of the RTE_CTE entries) and not by position in
		 * the cteList, and the moved CTEs are put first so they stay visible
		 * to the CTEs of the SELECT that reference them. If a name is used in
		 * both lists Postgres its shadowing semantics are lost, but luckily
		 * DuckDB errors on duplicate CTE names instead of silently picking
		 * one.
		 */
		if (copied_query->cteList != NIL) {
			IncrementVarSublevelsUp((Node *)select_query, -1, 1);
			select_query->cteList = list_concat(copied_query->cteList, select_query->cteList);
			select_query->hasRecursive = select_query->hasRecursive || copied_query->hasRecursive;
		}

		query_string = pgduckdb_get_querydef(select_query);
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
 * The DuckDB scan produces the output columns of the SELECT source, but the
 * ModifyTable node expects one value per table column, in the order of the
 * table definition and with exactly the types of those columns. The INSERT
 * might assign the SELECT columns to a subset of the table's columns and in a
 * different order than the table definition, so we cannot simply map them
 * positionally. Instead we use the Var in each entry of the INSERT its
 * targetlist, which references the SELECT column that is assigned to that
 * table column, and that matches the position of the DuckDB scan output.
 * Columns that the INSERT doesn't assign get their default expression that
 * the rewriter put in the targetlist, or NULL when there is none.
 */
static List *
ReconstructTargetListForInsert(TupleDesc pg_tupdesc, Query *query, List *duckdb_targetlist) {
	List *target_list = NIL;

	/*
	 * Find the range table index of the SELECT subquery, so we can recognize
	 * which Vars in the INSERT targetlist reference its output columns.
	 */
	int select_rti = FindInsertSelectRTI(query);

	/* Build one targetlist entry per table column, in table definition order. */
	for (int i = 0; i < pg_tupdesc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(pg_tupdesc, i);

		/*
		 * For dropped columns the ModifyTable node expects a NULL constant,
		 * see ExecCheckPlanOutput.
		 */
		if (attr->attisdropped) {
			TargetEntry *null_entry =
			    makeTargetEntry((Expr *)makeNullConst(INT4OID, -1, InvalidOid), attr->attnum, NULL, false);
			target_list = lappend(target_list, null_entry);
			continue;
		}

		/*
		 * Locate the entry in the INSERT its targetlist that assigns a value
		 * to this table column.
		 */
		TargetEntry *target_entry = NULL;
		foreach_node(TargetEntry, query_target_entry, query->targetList) {
			if (query_target_entry->resno != attr->attnum) {
				continue;
			}

			/*
			 * Check whether this expression references a SELECT column, and
			 * if so which one. That position matches the position in the
			 * DuckDB scan output.
			 */
			AttrNumber select_attno = 0;
			foreach_node(Var, var, pull_var_clause((Node *)query_target_entry->expr, 0)) {
				if (var->varno == select_rti) {
					select_attno = var->varattno;
					break;
				}
			}

			/*
			 * No SELECT column is referenced. Then the expression is either a
			 * default value that the rewriter put there, or an unknown-type
			 * constant that the parser inlined instead of creating a Var for
			 * it. In both cases the expression itself produces the value to
			 * insert and was already coerced to the column type, so we can
			 * use it directly.
			 */
			if (select_attno == 0) {
				target_entry = query_target_entry;
				break;
			}

			if (select_attno > list_length(duckdb_targetlist)) {
				elog(ERROR, "SELECT column assigned to column \"%s\" is missing from the DuckDB result",
				     NameStr(attr->attname));
			}

			/* Use the DuckDB scan column that the SELECT column maps to. */
			target_entry = list_nth_node(TargetEntry, duckdb_targetlist, select_attno - 1);
			target_entry->resno = attr->attnum;

			/*
			 * The types of the columns that DuckDB returns don't necessarily
			 * match the table column types exactly (e.g. a DuckDB VARCHAR
			 * always maps to text, even when the column is of type varchar),
			 * so add a cast when they differ. We use COERCION_ASSIGNMENT
			 * because that matches the semantics Postgres uses for INSERT:
			 * most importantly, assignment casts throw an error when a value
			 * doesn't fit the column type (e.g. a 6 character string into a
			 * varchar(3) column), while an explicit cast would silently
			 * truncate it.
			 */
			Oid source_type = exprType((Node *)target_entry->expr);
			int32 source_typmod = exprTypmod((Node *)target_entry->expr);
			if (source_type != attr->atttypid || source_typmod != attr->atttypmod) {
				Expr *coerced_expr =
				    (Expr *)coerce_to_target_type(NULL, (Node *)target_entry->expr, source_type, attr->atttypid,
				                                  attr->atttypmod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
				if (coerced_expr == NULL) {
					elog(ERROR, "cannot coerce column \"%s\" from type %s to type %s", NameStr(attr->attname),
					     format_type_be(source_type), format_type_be(attr->atttypid));
				}
				target_entry->expr = coerced_expr;
			}
			break;
		}

		if (target_entry) {
			target_list = lappend(target_list, target_entry);
			continue;
		}

		/*
		 * The rewriter already added targetlist entries with the default
		 * expression for all unassigned columns that have a default, so any
		 * column still missing from the targetlist gets a NULL.
		 */
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
		elog(elevel, "(PGDuckDB/CreatePlan) Prepared query returned an error: %s", prepared_query->GetError().c_str());
		return nullptr;
	}

	CustomScan *duckdb_node = makeNode(CustomScan);

	auto &prepared_result_types = prepared_query->GetTypes();

	for (size_t i = 0; i < prepared_result_types.size(); i++) {
		Oid postgresColumnOid = pgduckdb::GetPostgresDuckDBType(prepared_result_types[i], throw_error);

		if (!OidIsValid(postgresColumnOid)) {
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
		typtup->typtypmod = pgduckdb::GetPostgresDuckDBTypemod(prepared_result_types[i]);

		/*
		 * We use the invalid varno 0 here, because at this point we don't
		 * know yet at which position in the rtable of the final plan the RTE
		 * of this custom scan will end up. DuckdbPlanNode fills in the actual
		 * position with SetCustomScanVarno once it has built that rtable.
		 */
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

	if (IsAllowedPostgresInsert(query, true)) {
		RangeTblEntry *target_rel = (RangeTblEntry *)list_nth(query->rtable, query->resultRelation - 1);
		Relation rel = RelationIdGetRelation(target_rel->relid);
		TupleDesc pg_tupdesc = RelationGetDescr(rel);

		/*
		 * This also needs to happen when the column counts match, because an
		 * INSERT that specifies all columns can still assign them in a
		 * different order than the table definition.
		 */
		duckdb_node->scan.plan.targetlist =
		    ReconstructTargetListForInsert(pg_tupdesc, query, duckdb_node->scan.plan.targetlist);

		RelationClose(rel);
	}

	duckdb_node->custom_private = list_make1(query);
	duckdb_node->methods = &duckdb_scan_scan_methods;

	return (Plan *)duckdb_node;
}

/*
 * Updates the Vars in the custom_scan_tlist to point to the RTE of the
 * CustomScan, which CreatePlan left invalid because the position of that RTE
 * in the rtable is only known once the final plan is built.
 */
static void
SetCustomScanVarno(CustomScan *custom_scan, int varno) {
	foreach_node(TargetEntry, target_entry, custom_scan->custom_scan_tlist) {
		Var *var = castNode(Var, target_entry->expr);
		var->varno = varno;
	}
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

	/*
	 * For INSERTs into Postgres tables only the SELECT source of the INSERT
	 * runs in DuckDB. We let standard_planner build the normal INSERT plan,
	 * so that the ModifyTable node and the result relation metadata are all
	 * filled in correctly, and then replace the plan that produces the rows
	 * to insert with our CustomScan node.
	 */
	if (IsAllowedPostgresInsert(parse, true)) {
		Query *copied_query = (Query *)copyObjectImpl(parse);
#if PG_VERSION_NUM >= 190000
		PlannedStmt *postgres_plan = standard_planner(copied_query, NULL, cursor_options, NULL, NULL);
#else
		PlannedStmt *postgres_plan = standard_planner(copied_query, NULL, cursor_options, NULL);
#endif
		Assert(IsA(postgres_plan->planTree, ModifyTable));
		outerPlan(postgres_plan->planTree) = duckdb_plan;

		/* Put a DuckDB RTE at the end of the rtable */
		RangeTblEntry *insert_rte = DuckdbRangeTableEntry(custom_scan);
		postgres_plan->rtable = lappend(postgres_plan->rtable, insert_rte);
		SetCustomScanVarno(custom_scan, list_length(postgres_plan->rtable));

		return postgres_plan;
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
	SetCustomScanVarno(custom_scan, 1);
#if PG_VERSION_NUM >= 160000
	result->permInfos = NULL;
#endif
#if PG_VERSION_NUM >= 190000
	result->resultRelationRelids = NULL;
#else
	result->resultRelations = NULL;
#endif
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
