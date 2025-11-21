#pragma once

#include <string>

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

void ValidateExternalIdentifier(const char *identifier, const char *context);

std::string BuildExternalTableFunctionCall(const char *reader, const char *location, void *options);

bool EnsureExternalTableLoaded(Oid relid);

void ForgetLoadedExternalTable(Oid relid);

void ResetLoadedExternalTableCache();

bool pgduckdb_is_external_relation(Oid relation_oid);

const char *ExternalTableServerName();

} // namespace pgduckdb
