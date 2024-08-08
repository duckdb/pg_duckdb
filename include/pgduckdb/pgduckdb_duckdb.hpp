#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
#include "optimizer/optimizer.h"
}

namespace pgduckdb {

class Connection : public duckdb::Connection {
public:
	Connection(duckdb::DuckDB &db) : duckdb::Connection(db), tracked_replacement_scan_id(0) {
	}

	virtual ~Connection();

	void
	SetTrackedReplacementScanId(const uint32_t id) {
		tracked_replacement_scan_id = id;
	}

private:
	uint32_t tracked_replacement_scan_id;
};

class DuckDBManager {
public:
	static inline const DuckDBManager &
	Get() {
		static DuckDBManager instance;
		return instance;
	}

	inline duckdb::DuckDB &
	GetDatabase() const {
		return *database;
	}

private:
	DuckDBManager();
	void InitializeDatabase();

	void LoadSecrets(duckdb::ClientContext &);
	void LoadExtensions(duckdb::ClientContext &);
	void LoadFunctions(duckdb::ClientContext &);
	void ProcessRCFile(duckdb::ClientContext &);

	duckdb::unique_ptr<duckdb::DuckDB> database;
};

duckdb::unique_ptr<Connection> DuckdbCreateConnection(List *rtables, PlannerInfo *planner_info, List *needed_columns,
                                                      const char *query);
duckdb::unique_ptr<duckdb::QueryResult> RunQuery(duckdb::Connection const &connection, const std::string &query);

} // namespace pgduckdb
