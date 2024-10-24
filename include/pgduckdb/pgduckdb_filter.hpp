#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
}

namespace pgduckdb {

bool ApplyValueFilter(const duckdb::TableFilter &filter, const Datum &value, bool is_null, Oid type_oid);

} // namespace pgduckdb
