#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class PostgresStorageExtension : public StorageExtension {
public:
	PostgresStorageExtension();
};

} // namespace duckdb
