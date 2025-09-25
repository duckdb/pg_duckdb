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

void RegisterExternalTableDependency(Oid relid);

void EnsureExternalTablesCatalogExists();

void UpsertExternalTableMetadata(Oid relid, const char *reader, const char *location, Datum options);

void DeleteExternalTableMetadata(Oid relid);

} // namespace pgduckdb
