#pragma once

#include "duckdb/storage/storage_extension.hpp"

#include "generated/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

class PostgresStorageExtension : public duckdb::StorageExtension {
public:
	PostgresStorageExtension();
};

} // namespace pgduckdb
