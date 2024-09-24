#pragma once

namespace duckdb {
class DuckDB;
}

namespace pgduckdb {

void OnConnection(const int socket_fd, duckdb::DuckDB &db);

} // namespace pgduckdb
