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
	PostgresStorageExtensionInfo(Snapshot snapshot, PlannerInfo *planner_info)
	    : snapshot(snapshot), planner_info(planner_info) {
	}

public:
	Snapshot snapshot;
	PlannerInfo *planner_info;
};

class PostgresStorageExtension : public StorageExtension {
public:
	PostgresStorageExtension(Snapshot snapshot, PlannerInfo *planner_info);
};

} // namespace duckdb
