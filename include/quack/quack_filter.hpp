#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
}

namespace quack {

bool ApplyValueFilter(duckdb::TableFilter &filter, Datum &value, bool isNull, Oid typeOid);

} // namespace quack