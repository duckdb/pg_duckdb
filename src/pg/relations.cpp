#include "pgduckdb/pg/relations.hpp"

#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "access/relation.h"   // relation_open and relation_close
#include "optimizer/plancat.h" // estimate_rel_size
#include "utils/rel.h"
#include "utils/resowner.h" // CurrentResourceOwner and TopTransactionResourceOwner
}

namespace pgduckdb {

TupleDesc PDRelationGetDescr(Relation relation) {
	return relation->rd_att;
}

int GetTupleDescNatts(const TupleDesc tupleDesc) {
	return tupleDesc->natts;
}

const char *GetAttName(const Form_pg_attribute att) {
	return NameStr(att->attname);
}

Form_pg_attribute GetAttr(const TupleDesc tupleDesc, int i) {
	return &tupleDesc->attrs[i];
}

Relation OpenRelation(Oid relationId) {
	/*
	 * We always open & close the relation using the
	 * TopTransactionResourceOwner to avoid having to close the relation
	 * whenever Postgres switches resource owners, because opening a relation
	 * with one resource owner and closing it with another is not allowed.
	 */
	ResourceOwner saveResourceOwner = CurrentResourceOwner;
	CurrentResourceOwner = TopTransactionResourceOwner;
	auto rel = PostgresFunctionGuard(relation_open, relationId, AccessShareLock);
	CurrentResourceOwner = saveResourceOwner;
	return rel;
}

void CloseRelation(Relation rel) {
	/*
	 * We always open & close the relation using the
	 * TopTransactionResourceOwner to avoid having to close the relation
	 * whenever Postgres switches resource owners, because opening a relation
	 * with one resource owner and closing it with another is not allowed.
	 */
	ResourceOwner saveResourceOwner = CurrentResourceOwner;
	CurrentResourceOwner = TopTransactionResourceOwner;
	PostgresFunctionGuard(relation_close, rel, NoLock);

	CurrentResourceOwner = saveResourceOwner;
}

void EstimateRelSize(Relation rel, int32_t *attr_widths, BlockNumber *pages, double *tuples, double *allvisfrac) {
	::estimate_rel_size(rel, attr_widths, pages, tuples, allvisfrac);
}

} // namespace pgduckdb
