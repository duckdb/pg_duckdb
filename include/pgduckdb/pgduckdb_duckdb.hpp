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
	GetDatabase() {
		if(CheckSecretsSeq()) {
			auto connection = duckdb::make_uniq<duckdb::Connection>(*database);
			auto &context = *connection->context;
			DropSecrets(context);
			LoadSecrets(context);
		}
		return *database;
	}

	duckdb::unique_ptr<duckdb::Connection> GetConnection() const;

private:
	DuckDBManager();
	void InitializeDatabase();
	bool CheckSecretsSeq();
	void LoadSecrets(duckdb::ClientContext &);
	void DropSecrets(duckdb::ClientContext &);
	void LoadExtensions(duckdb::ClientContext &);
	void LoadFunctions(duckdb::ClientContext &);

	int secret_table_num_rows;
	int secret_table_current_seq;
	duckdb::unique_ptr<duckdb::DuckDB> database;
};

} // namespace pgduckdb
