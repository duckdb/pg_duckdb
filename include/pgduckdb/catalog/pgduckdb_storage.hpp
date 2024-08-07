#pragma once

#include "duckdb/storage/storage_extension.hpp"
extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/snapshot.h"
#include "nodes/pathnodes.h"
}

namespace pgduckdb {

class PostgresStorageExtensionInfo : public duckdb::StorageExtensionInfo {
public:
	PostgresStorageExtensionInfo(Snapshot snapshot, PlannerInfo *planner_info)
	    : snapshot(snapshot), planner_info(planner_info) {
	}

public:
	Snapshot snapshot;
	PlannerInfo *planner_info;
};

class PostgresStorageExtension : public duckdb::StorageExtension {
public:
	PostgresStorageExtension(Snapshot snapshot, PlannerInfo *planner_info);
};

} // namespace pgduckdb
