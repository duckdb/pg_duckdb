#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {
void InitUserDataCache();
bool IsMotherDuckEnabled();
Oid MotherDuckPostgresUser();
void InvalidateUserDataCache();
} // namespace pgduckdb
