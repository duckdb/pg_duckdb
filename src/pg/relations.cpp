#include "pgduckdb/pg/relations.hpp"

#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h" // GETSTRUCT
#include "access/relation.h"     // relation_open and relation_close
#include "catalog/namespace.h"   // makeRangeVarFromNameList, RangeVarGetRelid
#include "optimizer/plancat.h"   // estimate_rel_size
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/resowner.h"    // CurrentResourceOwner and TopTransactionResourceOwner
#include "executor/tuptable.h" // TupIsNull
#include "utils/syscache.h"    // RELOID
}

namespace pgduckdb {

#undef RelationGetDescr

#if PG_VERSION_NUM < 150000
// clang-format off

/*
 * Relation kinds with a table access method (rd_tableam).  Although sequences
 * use the heap table AM, they are enough of a special case in most uses that
 * they are not included here.  Likewise, partitioned tables can have an access
 * method defined so that their partitions can inherit it, but they do not set
 * rd_tableam; hence, this is handled specially outside of this macro.
 */
#define RELKIND_HAS_TABLE_AM(relkind) \
	((relkind) == RELKIND_RELATION || \
	 (relkind) == RELKIND_TOASTVALUE || \
	 (relkind) == RELKIND_MATVIEW)

// clang-format on
#endif

TupleDesc
RelationGetDescr(Relation rel) {
	return rel->rd_att;
}

int
GetTupleDescNatts(const TupleDesc tupleDesc) {
	return tupleDesc->natts;
}

const char *
GetAttName(const Form_pg_attribute att) {
	return NameStr(att->attname);
}

Form_pg_attribute
GetAttr(const TupleDesc tupleDesc, int i) {
	return &tupleDesc->attrs[i];
}

bool
TupleIsNull(TupleTableSlot *slot) {
	return TupIsNull(slot);
}

void
SlotGetAllAttrs(TupleTableSlot *slot) {
	PostgresFunctionGuard(slot_getallattrs, slot);
}

Relation
OpenRelation(Oid relationId) {
	if (PostgresFunctionGuard(check_enable_rls, relationId, InvalidOid, false) == RLS_ENABLED) {
		throw duckdb::NotImplementedException(
		    "Cannot use \"%s\" relation in a DuckDB query, because RLS is enabled on it",
		    PostgresFunctionGuard(get_rel_name, relationId));
	}

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

void
CloseRelation(Relation rel) {
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

double
EstimateRelSize(Relation rel) {
	Cardinality cardinality = 0;

	if (RELKIND_HAS_TABLE_AM(rel->rd_rel->relkind) || rel->rd_rel->relkind == RELKIND_INDEX) {
		BlockNumber pages;
		double allvisfrac;
		PostgresFunctionGuard(estimate_rel_size, rel, nullptr, &pages, &cardinality, &allvisfrac);
	}

	return cardinality;
}

Oid
PGGetRelidFromSchemaAndTable(const char *schema_name, const char *entry_name) {
	List *name_list = NIL;
	name_list = lappend(name_list, makeString(pstrdup(schema_name)));
	name_list = lappend(name_list, makeString(pstrdup(entry_name)));
	RangeVar *table_range_var = makeRangeVarFromNameList(name_list);
	return RangeVarGetRelid(table_range_var, AccessShareLock, true);
}

Oid
GetRelidFromSchemaAndTable(const char *schema_name, const char *entry_name) {
	return PostgresFunctionGuard(PGGetRelidFromSchemaAndTable, schema_name, entry_name);
}

bool
IsValidOid(Oid oid) {
	return oid != InvalidOid;
}

bool
IsValidBlockNumber(BlockNumber block_number) {
	return block_number != InvalidBlockNumber;
}

/*
 * generate_qualified_relation_name
 *		Compute the name to display for a relation specified by OID
 *
 * As above, but unconditionally schema-qualify the name.
 */
static char *
GenerateQualifiedRelationName_Unsafe(Relation rel) {
	char *nspname = get_namespace_name_or_temp(rel->rd_rel->relnamespace);
	if (!nspname)
		elog(ERROR, "cache lookup failed for namespace %u", rel->rd_rel->relnamespace);

	return quote_qualified_identifier(nspname, NameStr(rel->rd_rel->relname));
}

char *
GenerateQualifiedRelationName(Relation rel) {
	return PostgresFunctionGuard(GenerateQualifiedRelationName_Unsafe, rel);
}

const char *
QuoteIdentifier(const char *ident) {
	return PostgresFunctionGuard(quote_identifier, ident);
}

const char *
GetRelationName(Relation rel) {
	return RelationGetRelationName(rel);
}

} // namespace pgduckdb
