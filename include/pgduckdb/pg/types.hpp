#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb::pg {
bool IsArrayType(Oid type_oid);
bool IsDomainType(Oid type_oid);
bool IsArrayDomainType(Oid type_oid);
Oid GetBaseDuckColumnType(Oid attribute_type_oid);
Datum StringToNumeric(const char *str);
Datum StringToBitString(const char *str);
} // namespace pgduckdb::pg
