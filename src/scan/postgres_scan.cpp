#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/scan/postgres_table_reader.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

#include "pgduckdb/pgduckdb_process_lock.hpp"
#include "pgduckdb/logger.hpp"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "executor/tuptable.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

namespace pgduckdb {

//
// PostgresScanGlobalState
//

void
PostgresScanGlobalState::ConstructTableScanQuery(duckdb::TableFunctionInitInput &input) {
	/* SELECT COUNT(*) FROM */
	if (input.column_ids.size() == 1 && input.column_ids[0] == UINT64_MAX) {
		scan_query << "SELECT COUNT(*) FROM " << ConstructFullyQualifiedTableName();
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
			output_columns.emplace_back(projection_id, input.column_ids[projection_id] + 1);
		}
	} else {
		duckdb::idx_t output_index = 0;
		for (const auto &column_id : input.column_ids) {
			output_columns.emplace_back(output_index++, column_id + 1);
		}
	}

	scan_query << "SELECT ";

	bool first = true;
	for (auto const &[duckdb_scanned_index, attr_num] : output_columns) {
		if (!first) {
			scan_query << ", ";
		}
		first = false;
		auto attr = table_tuple_desc->attrs[attr_num - 1];
		scan_query << quote_identifier(attr.attname.data);
	}

	scan_query << " FROM " << ConstructFullyQualifiedTableName();

	first = true;

	for (auto const &[attr_num, duckdb_scanned_index] : columns_to_scan) {
		auto filter = column_filters[duckdb_scanned_index];
		if (filter) {
			if (first) {
				scan_query << " WHERE ";
			} else {
				scan_query << " AND ";
			}
			first = false;
			scan_query << " (";
			auto attr = table_tuple_desc->attrs[attr_num - 1];
			scan_query << filter->ToString(attr.attname.data).c_str();
			scan_query << ") ";
		}
	}
}

std::string
PostgresScanGlobalState::ConstructFullyQualifiedTableName() {
	return psprintf("%s.%s", quote_identifier(get_namespace_name_or_temp(get_rel_namespace(rel->rd_rel->oid))),
	                quote_identifier(get_rel_name(rel->rd_rel->oid)));
}

PostgresScanGlobalState::PostgresScanGlobalState(Snapshot snapshot, Relation rel, duckdb::TableFunctionInitInput &input)
    : snapshot(snapshot), rel(rel), table_tuple_desc(RelationGetDescr(rel)), count_tuples_only(false),
      total_row_count(0) {
	ConstructTableScanQuery(input);
	table_reader_global_state =
	    duckdb::make_shared_ptr<PostgresTableReader>(scan_query.str().c_str(), count_tuples_only);
	pd_log(DEBUG2, "(DuckDB/PostgresSeqScanGlobalState) Running %" PRIu64 " threads -- ", (uint64_t)MaxThreads());
}

PostgresScanGlobalState::~PostgresScanGlobalState() {
}

//
// PostgresScanLocalState
//

PostgresScanLocalState::PostgresScanLocalState(PostgresScanGlobalState *global_state)
    : global_state(global_state), exhausted_scan(false) {
}

PostgresScanLocalState::~PostgresScanLocalState() {
}

//
// PostgresSeqScanFunctionData
//

PostgresScanFunctionData::PostgresScanFunctionData(Relation rel, uint64_t cardinality, Snapshot snapshot)
    : rel(rel), cardinality(cardinality), snapshot(snapshot) {
}

PostgresScanFunctionData::~PostgresScanFunctionData() {
}

//
// PostgresScanFunction
//

PostgresScanTableFunction::PostgresScanTableFunction()
    : TableFunction("postgres_scan", {}, PostgresScanFunction, nullptr, PostgresScanInitGlobal, PostgresScanInitLocal) {
	named_parameters["cardinality"] = duckdb::LogicalType::UBIGINT;
	named_parameters["relid"] = duckdb::LogicalType::UINTEGER;
	named_parameters["snapshot"] = duckdb::LogicalType::POINTER;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
	cardinality = PostgresScanCardinality;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PostgresScanTableFunction::PostgresScanInitGlobal(__attribute__((unused)) duckdb::ClientContext &context,
                                                  duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresScanFunctionData>();
	auto global_state = duckdb::make_uniq<PostgresScanGlobalState>(bind_data.snapshot, bind_data.rel, input);
	return global_state;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PostgresScanTableFunction::PostgresScanInitLocal(__attribute__((unused)) duckdb::ExecutionContext &context,
                                                 __attribute__((unused)) duckdb::TableFunctionInitInput &input,
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

void
PostgresScanTableFunction::PostgresScanFunction(__attribute__((unused)) duckdb::ClientContext &context,
                                                duckdb::TableFunctionInput &data, duckdb::DataChunk &output) {
	auto &local_state = data.local_state->Cast<PostgresScanLocalState>();

	/* We have exhausted table scan */
	if (local_state.exhausted_scan) {
		SetOutputCardinality(output, local_state);
		return;
	}

	local_state.output_vector_size = 0;

	int i = 0;
	for (; i < STANDARD_VECTOR_SIZE; i++) {
		TupleTableSlot *slot = local_state.global_state->table_reader_global_state->GetNextTuple();
		if (TupIsNull(slot)) {
			local_state.exhausted_scan = true;
			break;
		}
		slot_getallattrs(slot);
		InsertTupleIntoChunk(output, local_state, slot);
	}

	/* If we finish before reading complete vector means that scan was exhausted. */
	if (i != STANDARD_VECTOR_SIZE) {
		local_state.exhausted_scan = true;
	}

	SetOutputCardinality(output, local_state);
}

duckdb::unique_ptr<duckdb::NodeStatistics>
PostgresScanTableFunction::PostgresScanCardinality(__attribute__((unused)) duckdb::ClientContext &context,
                                                   const duckdb::FunctionData *data) {
	auto &bind_data = data->Cast<PostgresScanFunctionData>();
	return duckdb::make_uniq<duckdb::NodeStatistics>(bind_data.cardinality, bind_data.cardinality);
}

} // namespace pgduckdb
