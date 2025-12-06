#pragma once

#include <string>

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

// The name of the foreign server that we use for DuckDB foreign tables.
#define DUCKDB_FOREIGN_SERVER_NAME "duckdb"

void ValidateForeignIdentifier(const char *identifier, const char *context);

std::string BuildForeignTableFunctionCall(const char *reader, const char *location, void *options);

bool EnsureForeignTableLoaded(Oid relid);

void UnloadedForeignTable(Oid relid);

void ResetForeignTableCache();

bool IsForeignTableLoaded(Oid relid);

bool pgduckdb_is_foreign_relation(Oid relation_oid);

const char *ForeignTableServerName();

} // namespace pgduckdb
