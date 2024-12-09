#pragma once

#include "duckdb.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

duckdb::unique_ptr<duckdb::TableRef> PostgresViewScan(duckdb::ClientContext &context,
                                                      duckdb::ReplacementScanInput &input,
                                                      duckdb::optional_ptr<duckdb::ReplacementScanData> data);

}
