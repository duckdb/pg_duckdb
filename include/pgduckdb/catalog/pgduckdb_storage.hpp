#pragma once

#include "duckdb/storage/storage_extension.hpp"
extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/snapshot.h"
#include "nodes/pathnodes.h"
}

namespace duckdb {

class PostgresStorageExtensionInfo : public StorageExtensionInfo {
public:
	PostgresStorageExtensionInfo(Snapshot snapshot)
	    : snapshot(snapshot) {
	}

public:
	Snapshot snapshot;
};

class PostgresStorageExtension : public StorageExtension {
public:
	PostgresStorageExtension(Snapshot snapshot);
};

} // namespace duckdb
