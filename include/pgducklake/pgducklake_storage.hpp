#pragma once

#include "duckdb/storage/storage_extension.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

class PgDuckLakeStorageExtension : public duckdb::StorageExtension {
public:
	PgDuckLakeStorageExtension();
};

} // namespace pgduckdb
