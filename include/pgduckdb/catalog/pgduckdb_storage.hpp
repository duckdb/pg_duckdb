#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace pgduckdb {

class PostgresStorageExtension : public duckdb::StorageExtension {
public:
	PostgresStorageExtension();
};

} // namespace pgduckdb
