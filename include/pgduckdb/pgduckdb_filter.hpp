#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
}

namespace pgduckdb {

bool ApplyValueFilter(duckdb::TableFilter &filter, Datum &value, bool isNull, Oid typeOid);

} // namespace pgduckdb