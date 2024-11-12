#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

class PostgresScanGlobalState;
class PostgresScanLocalState;

// DuckDB has date starting from 1/1/1970 while PG starts from 1/1/2000
constexpr int32_t PGDUCKDB_DUCK_DATE_OFFSET = 10957;
constexpr int64_t PGDUCKDB_DUCK_TIMESTAMP_OFFSET =
    static_cast<int64_t>(PGDUCKDB_DUCK_DATE_OFFSET) * static_cast<int64_t>(86400000000) /* USECS_PER_DAY */;

duckdb::LogicalType ConvertPostgresToDuckColumnType(Form_pg_attribute &attribute);
Oid GetPostgresDuckDBType(duckdb::LogicalType type);
int32_t GetPostgresDuckDBTypemod(duckdb::LogicalType type);
duckdb::Value ConvertPostgresParameterToDuckValue(Datum value, Oid postgres_type);
void ConvertPostgresToDuckValue(Oid attr_type, Datum value, duckdb::Vector &result, uint64_t offset);
bool ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, uint64_t col);
void InsertTupleIntoChunk(duckdb::DataChunk &output, duckdb::shared_ptr<PostgresScanGlobalState> scan_global_state,
                          duckdb::shared_ptr<PostgresScanLocalState> scan_local_state, HeapTupleData *tuple);

} // namespace pgduckdb
