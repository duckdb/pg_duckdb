#include "pgduckdb/pg/types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
extern "C" {
#include "postgres.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
}

namespace pgduckdb::pg {

bool
IsArrayType(Oid type_oid) {
	// inlined type_is_array
	return PostgresFunctionGuard(get_element_type, type_oid) != InvalidOid;
}

bool
IsDomainType(Oid type_oid) {
	return PostgresFunctionGuard(get_typtype, type_oid) == TYPTYPE_DOMAIN;
}

bool
IsArrayDomainType(Oid type_oid) {
	bool is_array_domain = false;
	if (IsDomainType(type_oid)) {
		if (PostgresFunctionGuard(get_base_element_type, type_oid) != InvalidOid) {
			is_array_domain = true;
		}
	}
	return is_array_domain;
}

static Oid
GetBaseDuckColumnType_C(Oid attribute_type_oid) {
	Oid typoid = attribute_type_oid;
	if (get_typtype(attribute_type_oid) == TYPTYPE_DOMAIN) {
		/* It is a domain type that needs to be reduced to its base type */
		typoid = getBaseType(attribute_type_oid);
	} else if (type_is_array(attribute_type_oid)) {
		Oid eltoid = get_base_element_type(attribute_type_oid);
		if (OidIsValid(eltoid) && get_typtype(eltoid) == TYPTYPE_DOMAIN) {
			/* When the member type of an array is domain, you need to build a base array type */
			typoid = get_array_type(getBaseType(eltoid));
		}
	}
	return typoid;
}

Oid
GetBaseDuckColumnType(Oid attribute_type_oid) {
	return PostgresFunctionGuard(GetBaseDuckColumnType_C, attribute_type_oid);
}

} // namespace pgduckdb::pg
