#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
#include "optimizer/optimizer.h"
}

namespace pgduckdb {

class DuckDBManager {
public:
	static inline DuckDBManager &
	Get() {
		static DuckDBManager instance;
		return instance;
	}

	inline duckdb::DuckDB &
	GetDatabase() const {
		return *database;
	}

	inline duckdb::Connection *
	GetConnection() const {
		connection->context->registered_state->Remove("postgres_scan");
		return connection.get();
	}

	duckdb::Connection *GetConnection(List *rtables, PlannerInfo *planner_info, List *needed_columns,
	                                  const char *query);

private:
	DuckDBManager();
	void InitializeDatabase();

	void LoadSecrets(duckdb::ClientContext &);
	void LoadExtensions(duckdb::ClientContext &);
	void LoadFunctions(duckdb::ClientContext &);

	duckdb::unique_ptr<duckdb::DuckDB> database;
	duckdb::unique_ptr<duckdb::Connection> connection;
};

} // namespace pgduckdb
