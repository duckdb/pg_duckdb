#include "duckdb.hpp"

extern "C" const char *
quack_duckdb_version() {
	return duckdb::DuckDB::LibraryVersion();
}