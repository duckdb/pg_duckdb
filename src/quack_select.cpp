#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "fmgr.h"

#include "access/genam.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/rel.h"

#include "quack/quack_select.h"
}

#include "quack/quack_heap_scan.hpp"
#include "quack/quack_types.hpp"
#include "quack/quack_memory_allocator.hpp"

namespace quack {

static duckdb::unique_ptr<duckdb::DuckDB>
quack_open_database() {
	duckdb::DBConfig config;
	// config.allocator = duckdb::make_uniq<duckdb::Allocator>(QuackAllocate, QuackFree, QuackReallocate, nullptr);
	return duckdb::make_uniq<duckdb::DuckDB>(nullptr, &config);
}

} // namespace quack

extern "C" bool
quack_execute_select(QueryDesc *query_desc, ScanDirection direction, uint64_t count) {
	auto db = quack::quack_open_database();

	/* Add heap tables */
	db->instance->config.replacement_scans.emplace_back(
	    quack::PostgresHeapReplacementScan,
	    duckdb::make_uniq_base<duckdb::ReplacementScanData, quack::PostgresHeapReplacementScanData>(query_desc));
	auto connection = duckdb::make_uniq<duckdb::Connection>(*db);

	// Add the postgres_scan inserted by the replacement scan
	auto &context = *connection->context;
	quack::PostgresHeapScanFunction heap_scan_fun;
	duckdb::CreateTableFunctionInfo heap_scan_info(heap_scan_fun);

	auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
	context.transaction.BeginTransaction();
	catalog.CreateTableFunction(context, &heap_scan_info);
	context.transaction.Commit();

	idx_t column_count;

	CmdType operation;
	DestReceiver *dest;

	TupleTableSlot *slot = NULL;

	// FIXME: try-catch ?

	duckdb::unique_ptr<duckdb::MaterializedQueryResult> res = nullptr;

	res = connection->Query(query_desc->sourceText);
	if (res->HasError()) {
		return false;
	}

	operation = query_desc->operation;
	dest = query_desc->dest;

	dest->rStartup(dest, operation, query_desc->tupDesc);

	slot = MakeTupleTableSlot(query_desc->tupDesc, &TTSOpsHeapTuple);
	column_count = res->ColumnCount();

	while (true) {

		auto chunk = res->Fetch();

		if (!chunk || chunk->size() == 0) {
			break;
		}

		for (idx_t row = 0; row < chunk->size(); row++) {
			ExecClearTuple(slot);

			for (idx_t col = 0; col < column_count; col++) {
				auto value = chunk->GetValue(col, row);
				if (value.IsNull()) {
					slot->tts_isnull[col] = true;
				} else {
					slot->tts_isnull[col] = false;
					quack::ConvertDuckToPostgresValue(slot, value, col);
				}
			}

			ExecStoreVirtualTuple(slot);

			dest->receiveSlot(slot, dest);

			for (idx_t i = 0; i < column_count; i++) {
				if (slot->tts_tupleDescriptor->attrs[i].attbyval == false) {
					pfree(DatumGetPointer(slot->tts_values[i]));
				}
			}
		}
	}
	dest->rShutdown(dest);
	return true;
}