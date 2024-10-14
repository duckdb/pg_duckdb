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
		if (!instance.database) {
			instance.Initialize();
		}
		return instance;
	}

	static duckdb::unique_ptr<duckdb::Connection> CreateConnection();

	inline const std::string &
	GetDefaultDBName() const {
		return default_dbname;
	}

private:
	DuckDBManager();

	void Initialize();

	void InitializeDatabase();
	bool CheckSecretsSeq();
	void LoadSecrets(duckdb::ClientContext &);
	void DropSecrets(duckdb::ClientContext &);
	void LoadExtensions(duckdb::ClientContext &);
	void LoadFunctions(duckdb::ClientContext &);

	int secret_table_num_rows;
	int secret_table_current_seq;
	duckdb::DuckDB *database;
	std::string default_dbname;
};

} // namespace pgduckdb
