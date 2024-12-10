#pragma once

#include "pgduckdb/pg/declarations.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.


namespace duckdb {
class TableFilter;
} // namespace duckdb

namespace pgduckdb {

bool ApplyValueFilter(const duckdb::TableFilter &filter, const Datum &value, bool is_null, Oid type_oid);

} // namespace pgduckdb
