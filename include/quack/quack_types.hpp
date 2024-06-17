#pragma once

#include "duckdb.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/extra_type_info.hpp"

extern "C" {
#include "postgres.h"
#include "executor/tuptable.h"
}

#include "quack/scan/postgres_scan.hpp"

namespace quack {

// DuckDB has date starting from 1/1/1970 while PG starts from 1/1/2000
constexpr int32_t QUACK_DUCK_DATE_OFFSET = 10957;
constexpr int64_t QUACK_DUCK_TIMESTAMP_OFFSET = INT64CONST(10957) * USECS_PER_DAY;

duckdb::LogicalType ConvertPostgresToDuckColumnType(Form_pg_attribute &attribute);
Oid GetPostgresDuckDBType(duckdb::LogicalType type);
void ConvertPostgresToDuckValue(Datum value, duckdb::Vector &result, idx_t offset);
void ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, idx_t col);
void InsertTupleIntoChunk(duckdb::DataChunk &output, duckdb::shared_ptr<PostgresScanGlobalState> scanGlobalState,
                          duckdb::shared_ptr<PostgresScanLocalState> scanLocalState, HeapTupleData *tuple);

} // namespace quack