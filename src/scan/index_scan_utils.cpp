#include "pgduckdb/scan/index_scan_utils.hpp"

namespace pgduckdb {

/*
 * fix_indexqual_operand
 *	  Convert an indexqual expression to a Var referencing the index column.
 *
 * We represent index keys by Var nodes having varno == INDEX_VAR and varattno
 * equal to the index's attribute number (index column position).
 *
 * Most of the code here is just for sanity cross-checking that the given
 * expression actually matches the index column it's claimed to.
 */
Node *
fix_indexqual_operand(Node *node, IndexOptInfo *index, int indexcol) {
	Var *result;
	int pos;
	ListCell *indexpr_item;

	/*
	 * Remove any binary-compatible relabeling of the indexkey
	 */
	if (IsA(node, RelabelType))
		node = (Node *)((RelabelType *)node)->arg;

	Assert(indexcol >= 0 && indexcol < index->ncolumns);

	if (index->indexkeys[indexcol] != 0) {
		/* It's a simple index column */
		if (IsA(node, Var) && ((Var *)node)->varno == index->rel->relid &&
		    ((Var *)node)->varattno == index->indexkeys[indexcol]) {
			result = (Var *)copyObjectImpl(node);
			result->varno = INDEX_VAR;
			result->varattno = indexcol + 1;
			return (Node *)result;
		} else
			elog(ERROR, "index key does not match expected index column");
	}

	/* It's an index expression, so find and cross-check the expression */
	indexpr_item = list_head(index->indexprs);
	for (pos = 0; pos < index->ncolumns; pos++) {
		if (index->indexkeys[pos] == 0) {
			if (indexpr_item == NULL)
				elog(ERROR, "too few entries in indexprs list");
			if (pos == indexcol) {
				Node *indexkey;

				indexkey = (Node *)lfirst(indexpr_item);
				if (indexkey && IsA(indexkey, RelabelType))
					indexkey = (Node *)((RelabelType *)indexkey)->arg;
				if (equal(node, indexkey)) {
					result = makeVar(INDEX_VAR, indexcol + 1, exprType((const Node *)lfirst(indexpr_item)), -1,
					                 exprCollation((const Node *)lfirst(indexpr_item)), 0);
					return (Node *)result;
				} else
					elog(ERROR, "index key does not match expected index column");
			}
			indexpr_item = lnext(index->indexprs, indexpr_item);
		}
	}

	/* Oops... */
	elog(ERROR, "index key does not match expected index column");
	return NULL; /* keep compiler quiet */
}

/*
 * fix_indexqual_clause
 *	  Convert a single indexqual clause to the form needed by the executor.
 *
 * We replace nestloop params here, and replace the index key variables
 * or expressions by index Var nodes.
 */
Node *
fix_indexqual_clause(PlannerInfo *root, IndexOptInfo *index, int indexcol, Node *clause, List *indexcolnos) {
	/*
	 * Replace any outer-relation variables with nestloop params.
	 *
	 * This also makes a copy of the clause, so it's safe to modify it
	 * in-place below.
	 */
	// clause = replace_nestloop_params(root, clause);

	if (IsA(clause, OpExpr)) {
		OpExpr *op = (OpExpr *)clause;
		/* Replace the indexkey expression with an index Var. */
		linitial(op->args) = fix_indexqual_operand((Node *)linitial(op->args), index, indexcol);
	} else if (IsA(clause, RowCompareExpr)) {
		RowCompareExpr *rc = (RowCompareExpr *)clause;
		ListCell *lca, *lcai;
		/* Replace the indexkey expressions with index Vars. */
		Assert(list_length(rc->largs) == list_length(indexcolnos));
		forboth(lca, rc->largs, lcai, indexcolnos) {
			lfirst(lca) = fix_indexqual_operand((Node *)lfirst(lca), index, lfirst_int(lcai));
		}
	} else if (IsA(clause, ScalarArrayOpExpr)) {
		ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *)clause;

		/* Replace the indexkey expression with an index Var. */
		linitial(saop->args) = fix_indexqual_operand((Node *)linitial(saop->args), index, indexcol);
	} else if (IsA(clause, NullTest)) {
		NullTest *nt = (NullTest *)clause;
		/* Replace the indexkey expression with an index Var. */
		nt->arg = (Expr *)fix_indexqual_operand((Node *)(Node *)nt->arg, index, indexcol);
	} else
		elog(ERROR, "unsupported indexqual type: %d", (int)nodeTag(clause));

	return clause;
}

} // namespace pgduckdb
