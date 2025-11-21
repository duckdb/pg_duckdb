#pragma once

#include <string>

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

/*
 * The name of the foreign server that we use for DuckDB foreign tables.
 */
#define DUCKDB_FOREIGN_SERVER_NAME "ddb_foreign_server"

void ValidateForeignIdentifier(const char *identifier, const char *context);

std::string BuildForeignTableFunctionCall(const char *reader, const char *location, void *options);

bool EnsureForeignTableLoaded(Oid relid);

void ForgetLoadedForeignTable(Oid relid);

void ResetLoadedForeignTableCache();

bool pgduckdb_is_foreign_relation(Oid relation_oid);

const char *ForeignTableServerName();

} // namespace pgduckdb
