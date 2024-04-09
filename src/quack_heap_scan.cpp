#include "duckdb/main/client_context.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"

extern "C" {
#include "postgres.h"

#include "miscadmin.h"

#include "access/tableam.h"
#include "executor/executor.h"
#include "parser/parse_type.h"
#include "tcop/utility.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
}

#include "quack/quack_heap_scan.hpp"
#include "quack/quack_types.hpp"

// Postgres Relation

PostgresRelation::PostgresRelation(RangeTblEntry *table) : rel(RelationIdGetRelation(table->relid)) {
}

PostgresRelation::~PostgresRelation() {
	if (IsValid()) {
		RelationClose(rel);
	}
}

Relation
PostgresRelation::GetRelation() {
	return rel;
}

bool
PostgresRelation::IsValid() const {
	return RelationIsValid(rel);
}

PostgresRelation::PostgresRelation(PostgresRelation &&other) : rel(other.rel) {
	other.rel = nullptr;
}

namespace quack {

// ------- Table Function -------

PostgresScanFunction::PostgresScanFunction()
    : TableFunction("quack_postgres_scan", {}, PostgresFunc, PostgresBind, PostgresInitGlobal, PostgresInitLocal) {
	named_parameters["table"] = duckdb::LogicalType::POINTER;
	named_parameters["snapshot"] = duckdb::LogicalType::POINTER;
}

// Bind Data

PostgresScanFunctionData::PostgresScanFunctionData(PostgresRelation &&relation, Snapshot snapshot)
    : relation(std::move(relation)), snapshot(snapshot) {
}

PostgresScanFunctionData::~PostgresScanFunctionData() {
}

duckdb::unique_ptr<duckdb::FunctionData>
PostgresScanFunction::PostgresBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                   duckdb::vector<duckdb::LogicalType> &return_types,
                                   duckdb::vector<duckdb::string> &names) {
	auto table = (reinterpret_cast<RangeTblEntry *>(input.named_parameters["table"].GetPointer()));
	auto snapshot = (reinterpret_cast<Snapshot>(input.named_parameters["snapshot"].GetPointer()));

	D_ASSERT(table->relid);
	auto rel = PostgresRelation(table);

	auto tupleDesc = RelationGetDescr(rel.GetRelation());
	if (!tupleDesc) {
		elog(ERROR, "Failed to get tuple descriptor for relation with OID %u", table->relid);
		return nullptr;
	}

	int column_count = tupleDesc->natts;

	for (idx_t i = 0; i < column_count; i++) {
		Form_pg_attribute attr = &tupleDesc->attrs[i];
		Oid type_oid = attr->atttypid;
		auto col_name = duckdb::string(NameStr(attr->attname));
		auto duck_type = ConvertPostgresToDuckColumnType(type_oid);
		return_types.push_back(duck_type);
		names.push_back(col_name);
		/* Log column name and type */
		elog(INFO, "Column name: %s, Type: %s", col_name.c_str(), duck_type.ToString().c_str());
	}

	// FIXME: check this in the replacement scan
	D_ASSERT(rel.GetRelation()->rd_amhandler != 0);
	// These are the methods we need to interact with the table
	auto access_method_handler = GetTableAmRoutine(rel.GetRelation()->rd_amhandler);

	return duckdb::make_uniq<PostgresScanFunctionData>(std::move(rel), snapshot);
}

// Global State

PostgresScanGlobalState::PostgresScanGlobalState() {
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PostgresScanFunction::PostgresInitGlobal(duckdb::ClientContext &context, duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PostgresScanFunctionData>();
	(void)bind_data;
	// FIXME: we'll call 'parallelscan_initialize' here to initialize a parallel scan
	return duckdb::make_uniq<PostgresScanGlobalState>();
}

// Local State

PostgresScanLocalState::PostgresScanLocalState(PostgresRelation &relation, Snapshot snapshot) {
	auto rel = relation.GetRelation();
	tableam = rel->rd_tableam;

	// Initialize the scan state
	uint32 flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;
	scanDesc = rel->rd_tableam->scan_begin(rel, snapshot, 0, NULL, NULL, flags);
}
PostgresScanLocalState::~PostgresScanLocalState() {
	// Close the scan state
	tableam->scan_end(scanDesc);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PostgresScanFunction::PostgresInitLocal(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                        duckdb::GlobalTableFunctionState *gstate) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresScanFunctionData>();
	auto &relation = bind_data.relation;
	auto snapshot = bind_data.snapshot;
	return duckdb::make_uniq<PostgresScanLocalState>(relation, snapshot);
}

static void
InsertTupleIntoChunk(duckdb::DataChunk &output, TupleDesc tuple, TupleTableSlot *slot, idx_t offset) {
	for (int i = 0; i < tuple->natts; i++) {
		auto &result = output.data[i];
		Datum value = slot_getattr(slot, i + 1, &slot->tts_isnull[i]);
		if (slot->tts_isnull[i]) {
			auto &array_mask = duckdb::FlatVector::Validity(result);
			array_mask.SetInvalid(offset);
		} else {
			ConvertPostgresToDuckValue(value, result, offset);
		}
	}
}

void
PostgresScanFunction::PostgresFunc(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                   duckdb::DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<PostgresScanFunctionData>();
	auto &lstate = data_p.local_state->Cast<PostgresScanLocalState>();
	auto &gstate = data_p.global_state->Cast<PostgresScanGlobalState>();

	auto &relation = data.relation;
	auto snapshot = data.snapshot;
	auto &exhausted_scan = lstate.exhausted_scan;

	auto rel = relation.GetRelation();

	TupleDesc tupleDesc = RelationGetDescr(rel);
	auto scanDesc = lstate.scanDesc;

	auto slot = table_slot_create(rel, NULL);
	idx_t count = 0;
	for (; count < STANDARD_VECTOR_SIZE && !exhausted_scan; count++) {
		auto has_tuple = rel->rd_tableam->scan_getnextslot(scanDesc, ForwardScanDirection, slot);
		if (!has_tuple) {
			exhausted_scan = true;
			break;
		}
		// Received a tuple, insert it into the DataChunk
		InsertTupleIntoChunk(output, tupleDesc, slot, count);
	}
	ExecDropSingleTupleTableSlot(slot);
	output.SetCardinality(count);
}


PostgresReplacementScanData::PostgresReplacementScanData(QueryDesc *desc) : desc(desc) {
}
PostgresReplacementScanData::~PostgresReplacementScanData() {
}

static RangeTblEntry *
FindMatchingRelation(List *tables, const duckdb::string &to_find) {
	ListCell *lc;
	foreach (lc, tables) {
		RangeTblEntry *table = (RangeTblEntry *)lfirst(lc);
		if (table->relid) {
			auto rel = RelationIdGetRelation(table->relid);

			if (!RelationIsValid(rel)) {
				elog(ERROR, "Relation with OID %u is not valid", table->relid);
				return nullptr;
			}

			char *relName = RelationGetRelationName(rel);
			auto table_name = std::string(relName);
			if (duckdb::StringUtil::CIEquals(table_name, to_find)) {
				if (!rel->rd_amhandler) {
					// This doesn't have an access method handler, we cant read from this
					RelationClose(rel);
					return nullptr;
				}
				RelationClose(rel);
				return table;
			}
			RelationClose(rel);
		}
	}
	return nullptr;
}

duckdb::unique_ptr<duckdb::TableRef>
PostgresReplacementScan(duckdb::ClientContext &context, const duckdb::string &table_name,
                        duckdb::ReplacementScanData *data) {

	auto &scan_data = reinterpret_cast<PostgresReplacementScanData &>(*data);

	auto tables = scan_data.desc->plannedstmt->rtable;
	auto table = FindMatchingRelation(tables, table_name);

	if (!table) {
		elog(WARNING, "Failed to find table %s in replacement scan lookup", table_name.c_str());
		return nullptr;
	}

	// Then inside the table function we can scan tuples from the postgres table and convert them into duckdb vectors.
	auto table_function = duckdb::make_uniq<duckdb::TableFunctionRef>();
	duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("table"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::POINTER(duckdb::CastPointerToValue(table)))));

	children.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
	    duckdb::ExpressionType::COMPARE_EQUAL, duckdb::make_uniq<duckdb::ColumnRefExpression>("snapshot"),
	    duckdb::make_uniq<duckdb::ConstantExpression>(
	        duckdb::Value::POINTER(duckdb::CastPointerToValue(scan_data.desc->estate->es_snapshot)))));

	table_function->function = duckdb::make_uniq<duckdb::FunctionExpression>("quack_postgres_scan", std::move(children));

	return std::move(table_function);
}

} // namespace quack
