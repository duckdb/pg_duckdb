#include "pgduckdb/pg/relations.hpp"

#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h" // GETSTRUCT
#include "access/relation.h"     // relation_open and relation_close
#include "access/tupdesc.h"  // TupleDescAttr
#include "catalog/namespace.h"   // makeRangeVarFromNameList, RangeVarGetRelid
#include "catalog/pg_type.h" // Form_pg_type
#include "optimizer/plancat.h"   // estimate_rel_size
#include "utils/rel.h"
#include "utils/resowner.h" // CurrentResourceOwner and TopTransactionResourceOwner
#include "utils/syscache.h" // RELOID
#include "utils/lsyscache.h" // getBaseType
}

namespace pgduckdb {

#undef RelationGetDescr

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

Relation
OpenRelation(Oid relationId) {
	/*
	 * We always open & close the relation using the
	 * TopTransactionResourceOwner to avoid having to close the relation
	 * whenever Postgres switches resource owners, because opening a relation
	 * with one resource owner and closing it with another is not allowed.
	 */
	ResourceOwner saveResourceOwner = CurrentResourceOwner;
	CurrentResourceOwner = TopTransactionResourceOwner;
	auto rel = PostgresFunctionGuard(relation_open, relationId, AccessShareLock);
	TupleDesc tupleDesc = rel->rd_att;
	for (int i = 0; i < tupleDesc->natts; i++) {
		Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, i);
		if (get_typtype(thisatt->atttypid) == TYPTYPE_DOMAIN) {
			/* If the domain is an array type, you need to obtain the corresponding array dimension information */
			if (type_is_array_domain(thisatt->atttypid) && thisatt->attndims == 0) {
				HeapTuple  typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(thisatt->atttypid));
				thisatt->attndims = ((Form_pg_type) GETSTRUCT(typeTuple))->typndims;
				ReleaseSysCache(typeTuple);
			}

			/* It is a domain type that needs to be reduced to its base type */
			thisatt->atttypid = getBaseType(thisatt->atttypid);
		} else if (type_is_array(thisatt->atttypid)) {
			Oid elem_type = get_base_element_type(thisatt->atttypid);
			if (OidIsValid(elem_type) && get_typtype(elem_type) == TYPTYPE_DOMAIN) {
				/* When the member type of an array is domain, you need to build a base array type */
				thisatt->atttypid = get_array_type(getBaseType(elem_type));
			}
		}
	}

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

void
EstimateRelSize(Relation rel, int32_t *attr_widths, BlockNumber *pages, double *tuples, double *allvisfrac) {
	PostgresFunctionGuard(estimate_rel_size, rel, attr_widths, pages, tuples, allvisfrac);
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
IsRelView(Relation rel) {
	return rel->rd_rel->relkind == RELKIND_VIEW;
}

bool
IsValidBlockNumber(BlockNumber block_number) {
	return block_number != InvalidBlockNumber;
}

} // namespace pgduckdb
