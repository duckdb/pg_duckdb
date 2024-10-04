#pragma once

#include "duckdb/storage/storage_extension.hpp"
extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/snapshot.h"
#include "nodes/pathnodes.h"
}

namespace duckdb {

class PostgresStorageExtension : public StorageExtension {
public:
	PostgresStorageExtension();
};

} // namespace duckdb
