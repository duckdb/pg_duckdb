#include "duckdb/planner/filter/optional_filter.hpp"

#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/scan/postgres_table_reader.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/pg/memory.hpp"
#include "pgduckdb/pg/relations.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"

#include "pgduckdb/pgduckdb_process_lock.hpp"
#include "pgduckdb/logger.hpp"

#include <numeric> // std::accumulate

namespace pgduckdb {

//
// PostgresScanGlobalState
//

static duckdb::string
FilterJoin(duckdb::vector<duckdb::string> &filters, duckdb::string &&delimiter) {
	return std::accumulate(filters.begin() + 1, filters.end(), filters[0],
	                       [&delimiter](duckdb::string l, duckdb::string r) { return l + delimiter + r; });
}

int
PostgresScanGlobalState::ExtractQueryFilters(duckdb::TableFilter *filter, const char *column_name,
                                             duckdb::string &query_filters, bool is_inside_optional_filter) {
	switch (filter->filter_type) {
	case duckdb::TableFilterType::CONSTANT_COMPARISON:
	case duckdb::TableFilterType::IS_NULL:
	case duckdb::TableFilterType::IS_NOT_NULL:
	case duckdb::TableFilterType::IN_FILTER: {
		query_filters += filter->ToString(column_name).c_str();
		return 1;
	}
	case duckdb::TableFilterType::CONJUNCTION_OR:
	case duckdb::TableFilterType::CONJUNCTION_AND: {
		auto conjuction_filter = reinterpret_cast<duckdb::ConjunctionFilter *>(filter);
		duckdb::vector<std::string> conjuction_child_filters;
		for (idx_t i = 0; i < conjuction_filter->child_filters.size(); i++) {
			std::string child_filter;
			if (ExtractQueryFilters(conjuction_filter->child_filters[i].get(), column_name, child_filter,
			                        is_inside_optional_filter)) {
				conjuction_child_filters.emplace_back(child_filter);
			}
		}
		duckdb::string conjuction_delimiter =
		    filter->filter_type == duckdb::TableFilterType::CONJUNCTION_OR ? " OR " : " AND ";
		if (conjuction_child_filters.size()) {
			query_filters += "(" + FilterJoin(conjuction_child_filters, std::move(conjuction_delimiter)) + ")";
		}
		return conjuction_child_filters.size();
	}
	case duckdb::TableFilterType::OPTIONAL_FILTER: {
		auto optional_filter = reinterpret_cast<duckdb::OptionalFilter *>(filter);
		return ExtractQueryFilters(optional_filter->child_filter.get(), column_name, query_filters, true);
	}
	/* DYNAMIC_FILTER is push down filter from topN execution. STRUCT_EXTRACT is
	 * only received if struct_extract function is used. Default will catch all
	 * filter that could be added in future in DuckDB.
	 */
	case duckdb::TableFilterType::DYNAMIC_FILTER:
	case duckdb::TableFilterType::STRUCT_EXTRACT:
	default: {
		if (is_inside_optional_filter) {
			pd_log(DEBUG1, "(DuckDB/ExtractQueryFilters) Unsupported optional filter: %s",
			       filter->ToString(column_name).c_str());
			return 0;
		}
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR,
		                        "Invalid Filter Type: " + filter->ToString(column_name));
	}
	}
}

void
PostgresScanGlobalState::ConstructTableScanQuery(const duckdb::TableFunctionInitInput &input) {
	/* SELECT COUNT(*) FROM */
	if (input.column_ids.size() == 1 && input.column_ids[0] == UINT64_MAX) {
		scan_query << "SELECT COUNT(*) FROM " << pgduckdb::GenerateQualifiedRelationName(rel);
		count_tuples_only = true;
		return;
	}
	/*
	 * We need to read columns from the Postgres tuple in column order, but for
	 * outputting them we care about the DuckDB order. A map automatically
	 * orders them based on key, which in this case is the Postgres column
	 * order
	 */
	duckdb::map<AttrNumber, duckdb::idx_t> pg_column_order;
	duckdb::idx_t scan_index = 0;
	for (const auto &pg_column : input.column_ids) {
		/* Postgres AttrNumbers are 1-based */
		pg_column_order[pg_column + 1] = scan_index++;
	}

	auto table_filters = input.filters.get();

	std::vector<duckdb::pair<AttrNumber, duckdb::idx_t>> columns_to_scan;
	std::vector<duckdb::TableFilter *> column_filters(input.column_ids.size(), 0);

	for (auto const &[att_num, duckdb_scanned_index] : pg_column_order) {
		columns_to_scan.emplace_back(att_num, duckdb_scanned_index);

		if (!table_filters) {
			continue;
		}

		auto column_filter_it = table_filters->filters.find(duckdb_scanned_index);
		if (column_filter_it != table_filters->filters.end()) {
			column_filters[duckdb_scanned_index] = column_filter_it->second.get();
		}
	}

	/* We need to check do we consider projection_ids or column_ids list to be used
	 * for writing to output vector. Projection ids list will be used when
	 * columns that are used for query filtering are not used afterwards; otherwise
	 * column ids list will be used and all read tuple columns need to passed
	 * to upper layers of query execution.
	 */
	if (input.CanRemoveFilterColumns()) {
		for (const auto &projection_id : input.projection_ids) {
			output_columns.emplace_back(input.column_ids[projection_id] + 1);
		}
	} else {
		for (const auto &column_id : input.column_ids) {
			output_columns.emplace_back(column_id + 1);
		}
	}

	scan_query << "SELECT ";

	bool first = true;
	for (auto const &attr_num : output_columns) {
		if (!first) {
			scan_query << ", ";
		}
		first = false;
		auto attr = GetAttr(table_tuple_desc, attr_num - 1);
		scan_query << pgduckdb::QuoteIdentifier(GetAttName(attr));
	}

	scan_query << " FROM " << GenerateQualifiedRelationName(rel);

	duckdb::vector<duckdb::string> query_filters;
	for (auto const &[attr_num, duckdb_scanned_index] : columns_to_scan) {
		auto filter = column_filters[duckdb_scanned_index];
		if (!filter) {
			continue;
		}
		duckdb::string column_query_filters;
		auto attr = GetAttr(table_tuple_desc, attr_num - 1);
		auto col = pgduckdb::QuoteIdentifier(GetAttName(attr));
		if (ExtractQueryFilters(filter, col, column_query_filters, false)) {
			query_filters.emplace_back(column_query_filters);
		};
	}

	if (query_filters.size()) {
		scan_query << " WHERE ";
		scan_query << FilterJoin(query_filters, " AND ");
	}
}

PostgresScanGlobalState::PostgresScanGlobalState(Snapshot _snapshot, Relation _rel,
                                                 const duckdb::TableFunctionInitInput &input)
    : snapshot(_snapshot), rel(_rel), table_tuple_desc(RelationGetDescr(rel)), count_tuples_only(false),
      total_row_count(0) {
	ConstructTableScanQuery(input);
	table_reader_global_state =
	    duckdb::make_shared_ptr<PostgresTableReader>(scan_query.str().c_str(), count_tuples_only);
	// Dedicated Postgres memory context for temporary allocations during type conversion in scans. Current only used in
	// single-threaded scans.
	duckdb_scan_memory_ctx = pg::MemoryContextCreate(CurrentMemoryContext, "DuckdbScanContext");

	// Parallelism in scanning has two layers:
	//   1. The Postgres table_reader may launch parallel worker processes to scan the table.
	//   2. DuckDB can use multiple threads (controlled by max_threads) to consume results from the table_reader.
	//
	// By default, we restrict DuckDB to a single thread (max_threads = 1) in the following cases:
	//   - The scan is a count-only query (count_tuples_only).
	//   - The table_reader does not launch any parallel Postgres workers, so scanning is done in the current process.
	//   - The scan includes JSON or LIST columns, since parallelism is inefficient for these types. This is because
	//     converting these types requires calling Postgres functions, which use the Postgres memory context and
	//     require holding the global lock, limiting parallel efficiency.
	max_threads = 1;
	if (table_reader_global_state->NumWorkersLaunched() > 0 && !count_tuples_only) {
		max_threads = duckdb_threads_for_postgres_scan;
	}
	for (auto attr_num : output_columns) {
		Form_pg_attribute attr = GetAttr(table_tuple_desc, attr_num - 1);
		auto duckdb_type = ConvertPostgresToDuckColumnType(attr);
		// Set max_threads to 1 if the column is a JSON column
		if (duckdb_type.IsJSONType() || duckdb_type.id() == duckdb::LogicalTypeId::LIST) {
			max_threads = 1;
			pd_log(DEBUG2,
			       "(DuckDB/PostgresSeqScanGlobalState) Setting max_threads to 1 because of JSON or LIST column");
			break;
		}
	}

	pd_log(DEBUG2, "(DuckDB/PostgresSeqScanGlobalState) Running %" PRIu64 " threads -- ", (uint64_t)MaxThreads());
}

PostgresScanGlobalState::~PostgresScanGlobalState() {
}

//
// PostgresScanLocalState
//

PostgresScanLocalState::PostgresScanLocalState(PostgresScanGlobalState *_global_state)
    : global_state(_global_state), exhausted_scan(false) {
	slot = global_state->table_reader_global_state->InitTupleSlot();
}

PostgresScanLocalState::~PostgresScanLocalState() {
}

//
// PostgresSeqScanFunctionData
//

PostgresScanFunctionData::PostgresScanFunctionData(Relation _rel, uint64_t _cardinality, Snapshot _snapshot)
    : rel(_rel), cardinality(_cardinality), snapshot(_snapshot) {
}

PostgresScanFunctionData::~PostgresScanFunctionData() {
}

//
// PostgresScanFunction
//

PostgresScanTableFunction::PostgresScanTableFunction()
    : TableFunction("pgduckdb_postgres_scan", {}, PostgresScanFunction, nullptr, PostgresScanInitGlobal,
                    PostgresScanInitLocal) {
	named_parameters["cardinality"] = duckdb::LogicalType::UBIGINT;
	named_parameters["relid"] = duckdb::LogicalType::UINTEGER;
	named_parameters["snapshot"] = duckdb::LogicalType::POINTER;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
	cardinality = PostgresScanCardinality;
	to_string = ToString;
}

duckdb::InsertionOrderPreservingMap<duckdb::string>
PostgresScanTableFunction::ToString(duckdb::TableFunctionToStringInput &input) {
	auto &bind_data = input.bind_data->Cast<PostgresScanFunctionData>();
	duckdb::InsertionOrderPreservingMap<duckdb::string> result;
	result["Table"] = GetRelationName(bind_data.rel);
	return result;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PostgresScanTableFunction::PostgresScanInitGlobal(duckdb::ClientContext &, duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresScanFunctionData>();
	return duckdb::make_uniq<PostgresScanGlobalState>(bind_data.snapshot, bind_data.rel, input);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PostgresScanTableFunction::PostgresScanInitLocal(duckdb::ExecutionContext &, duckdb::TableFunctionInitInput &,
                                                 duckdb::GlobalTableFunctionState *gstate) {
	auto global_state = reinterpret_cast<PostgresScanGlobalState *>(gstate);
	return duckdb::make_uniq<PostgresScanLocalState>(global_state);
}
void
SetOutputCardinality(duckdb::DataChunk &output, PostgresScanLocalState &local_state) {
	idx_t output_cardinality =
	    local_state.output_vector_size <= STANDARD_VECTOR_SIZE ? local_state.output_vector_size : STANDARD_VECTOR_SIZE;
	output.SetCardinality(output_cardinality);
	local_state.output_vector_size -= output_cardinality;
}


/*
 * Fetches a single tuple from the underlying PostgreSQL table and appends it to the DuckDB output chunk.
 * This function is intended for use in single-threaded scans.
 *
 * @return True if a tuple was successfully fetched and inserted; false if there are no more tuples to scan.
 */
static bool
ScanSingleTuple(duckdb::DataChunk &output, PostgresScanLocalState &local_state) {
	TupleTableSlot *slot = local_state.global_state->table_reader_global_state->GetNextTuple();
	if (pgduckdb::TupleIsNull(slot)) {
		return false;
	}

	SlotGetAllAttrs(slot);
	// This memory context is use as a scratchpad space for any allocation required to add the tuple
	// into the chunk, such as decoding jsonb columns to their json string representation. We need to
	// only use this memory context here, and not for the full loop, because GetNextTuple() needs the
	// actual tuple to survive until the next call to GetNextTuple(), to be able to do index scans.
	// Cf. issue 796 and 802
	MemoryContext old_context = pg::MemoryContextSwitchTo(local_state.global_state->duckdb_scan_memory_ctx);
	InsertTupleIntoChunk(output, local_state, slot);
	pg::MemoryContextSwitchTo(old_context);
	return true;
}

void
PostgresScanTableFunction::PostgresScanFunction(duckdb::ClientContext &, duckdb::TableFunctionInput &data,
                                                duckdb::DataChunk &output) {
	auto &local_state = data.local_state->Cast<PostgresScanLocalState>();

	/* We have exhausted table scan */
	if (local_state.exhausted_scan) {
		SetOutputCardinality(output, local_state);
		return;
	}

	local_state.output_vector_size = 0;

	D_ASSERT(STANDARD_VECTOR_SIZE % LOCAL_STATE_SLOT_BATCH_SIZE == 0);
	bool is_parallel_scan = local_state.global_state->MaxThreads() > 1;
	int batch_size = is_parallel_scan ? LOCAL_STATE_SLOT_BATCH_SIZE : STANDARD_VECTOR_SIZE;
	int num_batches = STANDARD_VECTOR_SIZE / batch_size;

	// For single-threaded scans, only one batch is processed and the global lock is acquired for each batch.
	// For parallel scans, multiple batches are processed; the global lock is held only during tuple retrieval
	// from the PostgreSQL parallel worker, allowing the rest of the processing to proceed concurrently.
	for (size_t batch_idx = 0; batch_idx < num_batches; batch_idx++) {
		std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
		int valid_slots = 0;
		for (size_t i = 0; i < batch_size; i++) {
			bool ret = is_parallel_scan
							? local_state.global_state->table_reader_global_state->GetNextMinimalWorkerTuple(
									local_state.minimal_tuple_buffer[i])
							: ScanSingleTuple(output, local_state);
			if (!ret) {
				local_state.global_state->table_reader_global_state->PostgresTableReaderCleanup();
				local_state.exhausted_scan = true;
				break;
			}

			++valid_slots;
		}

		if (is_parallel_scan) {
			// The follow-up convertion logic is thread-safe, so release the lock to allow parallel execution.
			lock.unlock();
			for (size_t i = 0; i < valid_slots; i++) {
				MinimalTuple minmal_tuple = reinterpret_cast<MinimalTuple>(local_state.minimal_tuple_buffer[i].data());
				local_state.slot = ExecStoreMinimalTupleUnsafe(minmal_tuple, local_state.slot, false);
				SlotGetAllAttrs(local_state.slot);
				InsertTupleIntoChunk(output, local_state, local_state.slot);
			}
		} else {
			pg::MemoryContextReset(local_state.global_state->duckdb_scan_memory_ctx);
		}
	}

	SetOutputCardinality(output, local_state);
}

duckdb::unique_ptr<duckdb::NodeStatistics>
PostgresScanTableFunction::PostgresScanCardinality(duckdb::ClientContext &, const duckdb::FunctionData *data) {
	auto &bind_data = data->Cast<PostgresScanFunctionData>();
	return duckdb::make_uniq<duckdb::NodeStatistics>(bind_data.cardinality, bind_data.cardinality);
}

} // namespace pgduckdb
