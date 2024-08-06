#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
}

namespace pgduckdb {

bool ApplyValueFilter(duckdb::TableFilter &filter, Datum &value, bool is_null, Oid type_oid);

} // namespace pgduckdb
