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

bool TupleIsNull(TupleTableSlot *slot);

void SlotGetAllAttrs(TupleTableSlot *slot);

double EstimateRelSize(Relation rel);

Oid GetRelidFromSchemaAndTable(const char *, const char *);

bool IsValidOid(Oid);

bool IsValidBlockNumber(BlockNumber);

char *GenerateQualifiedRelationName(Relation rel);
const char *QuoteIdentifier(const char *ident);

const char *GetRelationName(Relation rel);

} // namespace pgduckdb
