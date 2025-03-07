#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "pgduckdb/pg/declarations.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

struct PostgresScanGlobalState;
struct PostgresScanLocalState;

// DuckDB has date starting from 1/1/1970 while PG starts from 1/1/2000
constexpr int32_t PGDUCKDB_DUCK_DATE_OFFSET = 10957;
constexpr int64_t PGDUCKDB_DUCK_TIMESTAMP_OFFSET =
    static_cast<int64_t>(PGDUCKDB_DUCK_DATE_OFFSET) * static_cast<int64_t>(86400000000) /* USECS_PER_DAY */;

// Check from regress/sql/date.sql
#define PG_MINYEAR (-4713)
#define PG_MINMONTH (11)
#define PG_MINDAY (24)
#define PG_MAXYEAR (5874897)
#define PG_MAXMONTH (12)
#define PG_MAXDAY (31)

const duckdb::date_t PGDUCKDB_PG_MIN_DATE_VALUE = duckdb::Date::FromDate(PG_MINYEAR,PG_MINMONTH,PG_MINDAY);
const duckdb::date_t PGDUCKDB_PG_MAX_DATE_VALUE = duckdb::Date::FromDate(PG_MAXYEAR,PG_MAXMONTH,PG_MAXDAY);

duckdb::LogicalType ConvertPostgresToDuckColumnType(Form_pg_attribute &attribute);
Oid GetPostgresDuckDBType(const duckdb::LogicalType &type);
int32_t GetPostgresDuckDBTypemod(const duckdb::LogicalType &type);
duckdb::Value ConvertPostgresParameterToDuckValue(Datum value, Oid postgres_type);
void ConvertPostgresToDuckValue(Oid attr_type, Datum value, duckdb::Vector &result, uint64_t offset);
bool ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, uint64_t col);
void InsertTupleIntoChunk(duckdb::DataChunk &output, PostgresScanLocalState &scan_local_state, TupleTableSlot *slot);

} // namespace pgduckdb
