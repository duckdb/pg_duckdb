#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

TupleDesc RelationGetDescr(Relation relation);

// Not thread-safe. Must be called under a lock.
Relation OpenRelation(Oid relationId);

// Not thread-safe. Must be called under a lock.
void CloseRelation(Relation relation);

int GetTupleDescNatts(const TupleDesc tupleDesc);

const char *GetAttName(const Form_pg_attribute);

Form_pg_attribute GetAttr(const TupleDesc tupleDesc, int i);

void EstimateRelSize(Relation rel, int32_t *attr_widths, BlockNumber *pages, double *tuples, double *allvisfrac);

Oid GetRelidFromSchemaAndTable(const char *, const char *);

bool IsValidOid(Oid);

bool IsValidBlockNumber(BlockNumber);

bool IsRelView(Relation);

} // namespace pgduckdb
