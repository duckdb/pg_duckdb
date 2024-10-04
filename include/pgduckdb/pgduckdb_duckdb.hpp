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

	duckdb::unique_ptr<duckdb::DuckDB> database;
};

duckdb::unique_ptr<duckdb::Connection> DuckdbCreateConnection(List *rtables, List *needed_columns, const char *query);

} // namespace pgduckdb
