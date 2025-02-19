#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb::pg {
bool IsArrayType(Oid type_oid);
}
