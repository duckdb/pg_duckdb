#pragma once

namespace duckdb {
class DatabaseInstance;
}

namespace pgduckdb {

/*
 * Registers a read-only file system for the system CA certificate bundles
 * that httpfs looks for. It lets httpfs verify TLS connections while
 * LocalFileSystem is disabled for roles without pg_read_server_files.
 */
void RegisterCACertificateFileSystem(duckdb::DatabaseInstance &db);

} // namespace pgduckdb
