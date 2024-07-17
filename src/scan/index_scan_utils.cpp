/*
 *
 */

extern "C"{
#include "postgres.h"
#include "optimizer/paramassign.h"
#include "optimizer/placeholder.h"
}

#include "pgduckdb/scan/index_scan_utils.hpp"


namespace pgduckdb {

static Node *
replace_nestloop_params_mutator(Node *node, PlannerInfo *root);

/*
 * replace_nestloop_params
 *	  Replace outer-relation Vars and PlaceHolderVars in the given expression
 *	  with nestloop Params
 *
 * All Vars and PlaceHolderVars belonging to the relation(s) identified by
 * root->curOuterRels are replaced by Params, and entries are added to
 * root->curOuterParams if not already present.
 */
static Node *
replace_nestloop_params(PlannerInfo *root, Node *expr)
{
	/* No setup needed for tree walk, so away we go */
	return replace_nestloop_params_mutator(expr, root);
}

static Node *
replace_nestloop_params_mutator(Node *node, PlannerInfo *root)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		/* Upper-level Vars should be long gone at this point */
		Assert(var->varlevelsup == 0);
		/* If not to be replaced, we can just return the Var unmodified */
		if (IS_SPECIAL_VARNO(var->varno) ||
			!bms_is_member(var->varno, root->curOuterRels))
			return node;
		/* Replace the Var with a nestloop Param */
		return (Node *) replace_nestloop_param_var(root, var);
	}
	if (IsA(node, PlaceHolderVar))
	{
		PlaceHolderVar *phv = (PlaceHolderVar *) node;

		/* Upper-level PlaceHolderVars should be long gone at this point */
		Assert(phv->phlevelsup == 0);

		/* Check whether we need to replace the PHV */
		if (!bms_is_subset(find_placeholder_info(root, phv)->ph_eval_at,
						   root->curOuterRels))
		{
			/*
			 * We can't replace the whole PHV, but we might still need to
			 * replace Vars or PHVs within its expression, in case it ends up
			 * actually getting evaluated here.  (It might get evaluated in
			 * this plan node, or some child node; in the latter case we don't
			 * really need to process the expression here, but we haven't got
			 * enough info to tell if that's the case.)  Flat-copy the PHV
			 * node and then recurse on its expression.
			 *
			 * Note that after doing this, we might have different
			 * representations of the contents of the same PHV in different
			 * parts of the plan tree.  This is OK because equal() will just
			 * match on phid/phlevelsup, so setrefs.c will still recognize an
			 * upper-level reference to a lower-level copy of the same PHV.
			 */
			PlaceHolderVar *newphv = makeNode(PlaceHolderVar);

			memcpy(newphv, phv, sizeof(PlaceHolderVar));
			newphv->phexpr = (Expr *)
				replace_nestloop_params_mutator((Node *) phv->phexpr,
												root);
			return (Node *) newphv;
		}
		/* Replace the PlaceHolderVar with a nestloop Param */
		return (Node *) replace_nestloop_param_placeholdervar(root, phv);
	}
	return expression_tree_mutator(node,
								   replace_nestloop_params_mutator,
								   (void *) root);
}

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
FixIndexQualOperand(Node *node, IndexOptInfo *index, int indexcol) {
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
FixIndexQualClause(PlannerInfo *root, IndexOptInfo *index, int indexcol, Node *clause, List *indexcolnos) {
	/*
	 * Replace any outer-relation variables with nestloop params.
	 *
	 * This also makes a copy of the clause, so it's safe to modify it
	 * in-place below.
	 */
	clause = replace_nestloop_params(root, clause);

	if (IsA(clause, OpExpr)) {
		OpExpr *op = (OpExpr *)clause;
		/* Replace the indexkey expression with an index Var. */
		linitial(op->args) = FixIndexQualOperand((Node *)linitial(op->args), index, indexcol);
	} else if (IsA(clause, RowCompareExpr)) {
		RowCompareExpr *rc = (RowCompareExpr *)clause;
		ListCell *lca, *lcai;
		/* Replace the indexkey expressions with index Vars. */
		Assert(list_length(rc->largs) == list_length(indexcolnos));
		forboth(lca, rc->largs, lcai, indexcolnos) {
			lfirst(lca) = FixIndexQualOperand((Node *)lfirst(lca), index, lfirst_int(lcai));
		}
	} else if (IsA(clause, ScalarArrayOpExpr)) {
		ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *)clause;

		/* Replace the indexkey expression with an index Var. */
		linitial(saop->args) = FixIndexQualOperand((Node *)linitial(saop->args), index, indexcol);
	} else if (IsA(clause, NullTest)) {
		NullTest *nt = (NullTest *)clause;
		/* Replace the indexkey expression with an index Var. */
		nt->arg = (Expr *)FixIndexQualOperand((Node *)(Node *)nt->arg, index, indexcol);
	} else
		elog(ERROR, "unsupported indexqual type: %d", (int)nodeTag(clause));

	return clause;
}

} // namespace pgduckdb
