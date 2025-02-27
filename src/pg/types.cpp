#include "pgduckdb/pg/types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
extern "C" {
#include "postgres.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/numeric.h"
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

static Datum
StringToNumeric_C(const char *str) {
	Datum pg_numeric = DirectFunctionCall3(numeric_in, CStringGetDatum(str), /*typelen=*/ObjectIdGetDatum(InvalidOid),
	                                       /*typmod=*/Int32GetDatum(-1));
	return pg_numeric;
}

Datum
StringToNumeric(const char *str) {
	return PostgresFunctionGuard(StringToNumeric_C, str);
}

static Datum
StringToBitString_C(const char *str) {
	Datum pg_varbit = DirectFunctionCall3(varbit_in, CStringGetDatum(str), /*typelen=*/ObjectIdGetDatum(VARBITOID),
	                                      /*typmod=*/Int32GetDatum(-1));
	return pg_varbit;
}

Datum
StringToBitString(const char *str) {
	return PostgresFunctionGuard(StringToBitString_C, str);
}

} // namespace pgduckdb::pg
