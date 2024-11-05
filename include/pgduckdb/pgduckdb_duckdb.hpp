#pragma once

#include "duckdb.hpp"

// FIXME: Make sure we don't need this
extern "C" {
#include "postgres.h"
#include "access/xact.h"
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

	static duckdb::Connection *GetConnection();

	inline const std::string &
	GetDefaultDBName() const {
		return default_dbname;
	}

	void
	Reset() {
		delete database;
		database = nullptr;
	}

private:
	DuckDBManager();

	void Initialize();

	void InitializeDatabase();

	void LoadSecrets(duckdb::ClientContext &);
	void DropSecrets(duckdb::ClientContext &);
	void LoadExtensions(duckdb::ClientContext &);
	void LoadFunctions(duckdb::ClientContext &);
	void RefreshConnectionState(duckdb::ClientContext &);

	static void DuckdbXactCallback_Cpp(XactEvent event, void *arg);
	static void DuckdbXactCallback(XactEvent event, void *arg);

	inline bool
	IsSecretSeqLessThan(int64_t seq) const {
		return secret_table_current_seq < seq;
	}

	inline bool
	IsExtensionsSeqLessThan(int64_t seq) const {
		return extensions_table_current_seq < seq;
	}

	inline void
	UpdateSecretSeq(int64_t seq) {
		secret_table_current_seq = seq;
	}

	inline void
	UpdateExtensionsSeq(int64_t seq) {
		extensions_table_current_seq = seq;
	}

	int secret_table_num_rows;
	int64_t secret_table_current_seq;
	int64_t extensions_table_current_seq;
	/*
	 * FIXME: Use a unique_ptr instead of a raw pointer. For now this is not
	 * possible though, as the MotherDuck extension causes an ABORT when the
	 * DuckDB database its destructor is run at the exit of the process.  This
	 * then in turn crashes Postgres, which we obviously dont't want. Not
	 * running the destructor also doesn't really have any downsides, as the
	 * process is going to die anyway. It's probably even a tiny bit more
	 * efficient not to run the destructor at all. But we should still fix
	 * this, because running the destructor is a good way to find bugs (such
	 * as the one reported in #279).
	 */
	duckdb::DuckDB *database;
	duckdb::unique_ptr<duckdb::Connection> connection;
	std::string default_dbname;
};

} // namespace pgduckdb
