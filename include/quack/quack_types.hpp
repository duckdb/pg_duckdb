#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "executor/tuptable.h"
}

#include "quack/quack_heap_seq_scan.hpp"

namespace quack {

duckdb::LogicalType ConvertPostgresToDuckColumnType(Oid type);
Oid GetPostgresDuckDBType(duckdb::LogicalTypeId type);
void ConvertPostgresToDuckValue(Datum value, duckdb::Vector &result, idx_t offset);
void ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, idx_t col);
void InsertTupleIntoChunk(duckdb::DataChunk &output, PostgresHeapSeqScanThreadInfo &threadScanInfo,
                          PostgresHeapSeqParallelScanState &parallelScanState);

} // namespace quack