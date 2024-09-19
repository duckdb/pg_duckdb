#include "duckdb.hpp"

#include "pgduckdb/scan/postgres_seq_scan.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

namespace pgduckdb {

//
// PostgresSeqScanGlobalState
//

PostgresSeqScanGlobalState::PostgresSeqScanGlobalState(Relation relation, duckdb::TableFunctionInitInput &input)
    : m_global_state(duckdb::make_shared_ptr<PostgresScanGlobalState>()),
      m_heap_reader_global_state(duckdb::make_shared_ptr<HeapReaderGlobalState>(relation)), m_relation(relation) {
	m_global_state->InitGlobalState(input);
	m_global_state->m_tuple_desc = RelationGetDescr(m_relation);
	elog(DEBUG2, "(PGDuckDB/PostgresSeqScanGlobalState) Running %lu threads -- ", MaxThreads());
}

PostgresSeqScanGlobalState::~PostgresSeqScanGlobalState() {
	if (m_relation) {
		RelationClose(m_relation);
	}
}

//
// PostgresSeqScanLocalState
//

PostgresSeqScanLocalState::PostgresSeqScanLocalState(Relation relation,
                                                     duckdb::shared_ptr<HeapReaderGlobalState> heap_reder_global_state,
                                                     duckdb::shared_ptr<PostgresScanGlobalState> global_state)
    : m_local_state(duckdb::make_shared_ptr<PostgresScanLocalState>()) {
	m_heap_table_reader = duckdb::make_uniq<HeapReader>(relation, heap_reder_global_state, global_state, m_local_state);
}

PostgresSeqScanLocalState::~PostgresSeqScanLocalState() {
}

//
// PostgresSeqScanFunctionData
//

PostgresSeqScanFunctionData::PostgresSeqScanFunctionData(uint64_t cardinality, Oid relid, Snapshot snapshot)
    : m_cardinality(cardinality), m_relid(relid), m_snapshot(snapshot) {
}

PostgresSeqScanFunctionData::~PostgresSeqScanFunctionData() {
}

//
// PostgresSeqScanFunction
//

PostgresSeqScanFunction::PostgresSeqScanFunction()
    : TableFunction("postgres_seq_scan", {}, PostgresSeqScanFunc, PostgresSeqScanBind, PostgresSeqScanInitGlobal,
                    PostgresSeqScanInitLocal) {
	named_parameters["cardinality"] = duckdb::LogicalType::UBIGINT;
	named_parameters["relid"] = duckdb::LogicalType::UINTEGER;
	named_parameters["snapshot"] = duckdb::LogicalType::POINTER;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
	cardinality = PostgresSeqScanCardinality;
}

duckdb::unique_ptr<duckdb::FunctionData>
PostgresSeqScanFunction::PostgresSeqScanBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                             duckdb::vector<duckdb::LogicalType> &return_types,
                                             duckdb::vector<duckdb::string> &names) {
	auto cardinality = input.named_parameters["cardinality"].GetValue<uint64_t>();
	auto relid = input.named_parameters["relid"].GetValue<uint32_t>();
	auto snapshot = (reinterpret_cast<Snapshot>(input.named_parameters["snapshot"].GetPointer()));

	auto rel = RelationIdGetRelation(relid);
	auto relation_descr = RelationGetDescr(rel);

	if (!relation_descr) {
		elog(WARNING, "(PGDuckDB/PostgresSeqScanBind) Failed to get tuple descriptor for relation with OID %u", relid);
		RelationClose(rel);
		return nullptr;
	}

	for (int i = 0; i < relation_descr->natts; i++) {
		Form_pg_attribute attr = &relation_descr->attrs[i];
		auto col_name = duckdb::string(NameStr(attr->attname));
		auto duck_type = ConvertPostgresToDuckColumnType(attr);
		return_types.push_back(duck_type);
		names.push_back(col_name);
		/* Log column name and type */
		elog(DEBUG2, "(PGDuckDB/PostgresSeqScanBind) Column name: %s, Type: %s --", col_name.c_str(),
		     duck_type.ToString().c_str());
	}

	RelationClose(rel);
	return duckdb::make_uniq<PostgresSeqScanFunctionData>(cardinality, relid, snapshot);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PostgresSeqScanFunction::PostgresSeqScanInitGlobal(duckdb::ClientContext &context,
                                                   duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresSeqScanFunctionData>();
	auto global_state = duckdb::make_uniq<PostgresSeqScanGlobalState>(RelationIdGetRelation(bind_data.m_relid), input);
	global_state->m_global_state->m_snapshot = bind_data.m_snapshot;
	global_state->m_relid = bind_data.m_relid;
	return std::move(global_state);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PostgresSeqScanFunction::PostgresSeqScanInitLocal(duckdb::ExecutionContext &context,
                                                  duckdb::TableFunctionInitInput &input,
                                                  duckdb::GlobalTableFunctionState *gstate) {
	auto global_state = reinterpret_cast<PostgresSeqScanGlobalState *>(gstate);
	return duckdb::make_uniq<PostgresSeqScanLocalState>(
	    global_state->m_relation, global_state->m_heap_reader_global_state, global_state->m_global_state);
}

void
PostgresSeqScanFunction::PostgresSeqScanFunc(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
                                             duckdb::DataChunk &output) {
	auto &local_state = data.local_state->Cast<PostgresSeqScanLocalState>();

	local_state.m_local_state->m_output_vector_size = 0;

	/* We have exhausted seq scan of heap table so we can return */
	if (local_state.m_local_state->m_exhausted_scan) {
		output.SetCardinality(0);
		return;
	}

	auto hasTuple = local_state.m_heap_table_reader->ReadPageTuples(output);

	if (!hasTuple || local_state.m_heap_table_reader->GetCurrentBlockNumber() == InvalidBlockNumber) {
		local_state.m_local_state->m_exhausted_scan = true;
	}
}

duckdb::unique_ptr<duckdb::NodeStatistics>
PostgresSeqScanFunction::PostgresSeqScanCardinality(duckdb::ClientContext &context, const duckdb::FunctionData *data) {
	auto &bind_data = data->Cast<PostgresSeqScanFunctionData>();
	return duckdb::make_uniq<duckdb::NodeStatistics>(bind_data.m_cardinality, bind_data.m_cardinality);
}

} // namespace pgduckdb
