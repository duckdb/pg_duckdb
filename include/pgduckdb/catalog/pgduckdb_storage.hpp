#pragma once

#include "duckdb/storage/storage_extension.hpp"
extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/snapshot.h"
}

namespace pgduckdb {

class PostgresStorageExtensionInfo : public duckdb::StorageExtensionInfo {
public:
	PostgresStorageExtensionInfo(Snapshot snapshot) : snapshot(snapshot) {
	}

public:
	Snapshot snapshot;
};

class PostgresStorageExtension : public duckdb::StorageExtension {
public:
	PostgresStorageExtension(Snapshot snapshot);
};

} // namespace pgduckdb
