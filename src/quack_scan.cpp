#include "duckdb/main/client_context.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"

#include "quack/quack.hpp"
#include "quack/quack_scan.hpp"

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

namespace duckdb {

// ------- Table Function -------

PostgresScanFunction::PostgresScanFunction()
    : TableFunction("postgres_scan", {}, PostgresFunc, PostgresBind, PostgresInitGlobal, PostgresInitLocal) {
	named_parameters["table"] = LogicalType::POINTER;
	named_parameters["snapshot"] = LogicalType::POINTER;
}

// Bind Data

PostgresScanFunctionData::PostgresScanFunctionData(PostgresRelation &&relation, Snapshot snapshot)
    : relation(std::move(relation)), snapshot(snapshot) {
}

PostgresScanFunctionData::~PostgresScanFunctionData() {
}

static LogicalType
PostgresToDuck(Oid type) {
	switch (type) {
	case BOOLOID:
		return LogicalTypeId::BOOLEAN;
	case CHAROID:
		return LogicalTypeId::TINYINT;
	case INT2OID:
		return LogicalTypeId::SMALLINT;
	case INT4OID:
		return LogicalTypeId::INTEGER;
	case INT8OID:
		return LogicalTypeId::BIGINT;
	case BPCHAROID:
	case TEXTOID:
	case VARCHAROID:
		return LogicalTypeId::VARCHAR;
	case DATEOID:
		return LogicalTypeId::DATE;
	case TIMESTAMPOID:
		return LogicalTypeId::TIMESTAMP;
	default:
		elog(ERROR, "Unsupported quack type: %d", type);
	}
}

unique_ptr<FunctionData>
PostgresScanFunction::PostgresBind(ClientContext &context, TableFunctionBindInput &input,
                                   vector<LogicalType> &return_types, vector<string> &names) {
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
		auto col_name = string(NameStr(attr->attname));
		auto duck_type = PostgresToDuck(type_oid);
		return_types.push_back(duck_type);
		names.push_back(col_name);

		/* Log column name and type */
		elog(INFO, "Column name: %s, Type: %s", col_name.c_str(), duck_type.ToString().c_str());
	}

	// FIXME: check this in the replacement scan
	D_ASSERT(rel.GetRelation()->rd_amhandler != 0);
	// These are the methods we need to interact with the table
	auto access_method_handler = GetTableAmRoutine(rel.GetRelation()->rd_amhandler);

	return make_uniq<PostgresScanFunctionData>(std::move(rel), snapshot);
}

// Global State

PostgresScanGlobalState::PostgresScanGlobalState() {
}

unique_ptr<GlobalTableFunctionState>
PostgresScanFunction::PostgresInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PostgresScanFunctionData>();
	auto &relation = bind_data.relation;
	// FIXME: we'll call 'parallelscan_initialize' here to initialize a parallel scan
	return make_uniq<PostgresScanGlobalState>();
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

unique_ptr<LocalTableFunctionState>
PostgresScanFunction::PostgresInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                        GlobalTableFunctionState *gstate) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresScanFunctionData>();
	auto &relation = bind_data.relation;
	auto snapshot = bind_data.snapshot;
	return make_uniq<PostgresScanLocalState>(relation, snapshot);
}

template <class T>
static void
Append(Vector &result, T value, idx_t offset) {
	auto data = FlatVector::GetData<T>(result);
	data[offset] = value;
}

static void
AppendString(Vector &result, Datum value, idx_t offset) {
	const char *text = VARDATA_ANY(value);
	int len = VARSIZE_ANY_EXHDR(value);
	string_t str(text, len);

	auto data = FlatVector::GetData<string_t>(result);
	data[offset] = StringVector::AddString(result, str);
}

// The table scan function
static void
ConvertDatumToDuckDB(Datum value, Vector &result, idx_t offset) {
	constexpr int32_t QUACK_DUCK_DATE_OFFSET = 10957;
	constexpr int64_t QUACK_DUCK_TIMESTAMP_OFFSET = INT64CONST(10957) * USECS_PER_DAY;

	switch (result.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		Append<bool>(result, DatumGetBool(value), offset);
		break;
	case LogicalTypeId::TINYINT:
		Append<int8_t>(result, DatumGetChar(value), offset);
		break;
	case LogicalTypeId::SMALLINT:
		Append<int16_t>(result, DatumGetInt16(value), offset);
		break;
	case LogicalTypeId::INTEGER:
		Append<int32_t>(result, DatumGetInt32(value), offset);
		break;
	case LogicalTypeId::BIGINT:
		Append<int64_t>(result, DatumGetInt64(value), offset);
		break;
	case LogicalTypeId::VARCHAR:
		AppendString(result, value, offset);
		break;
	case LogicalTypeId::DATE:
		Append<date_t>(result, date_t(static_cast<int32_t>(value + QUACK_DUCK_DATE_OFFSET)), offset);
		break;
	case LogicalTypeId::TIMESTAMP:
		Append<dtime_t>(result, dtime_t(static_cast<int64_t>(value + QUACK_DUCK_TIMESTAMP_OFFSET)), offset);
		break;
	default:
		elog(ERROR, "Unsupported quack type: %hhu", result.GetType().id());
		break;
	}
}

static void
InsertTupleIntoChunk(DataChunk &output, TupleDesc tuple, TupleTableSlot *slot, idx_t offset) {
	for (int i = 0; i < tuple->natts; i++) {
		auto &result = output.data[i];
		Datum value = slot_getattr(slot, i + 1, &slot->tts_isnull[i]);
		if (slot->tts_isnull[i]) {
			auto &array_mask = FlatVector::Validity(result);
			array_mask.SetInvalid(offset);
		} else {
			ConvertDatumToDuckDB(value, result, offset);
		}
	}
}

void
PostgresScanFunction::PostgresFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
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

// ------- Replacement Scan -------

PostgresReplacementScanData::PostgresReplacementScanData(QueryDesc *desc) : desc(desc) {
}
PostgresReplacementScanData::~PostgresReplacementScanData() {
}

static RangeTblEntry *
FindMatchingRelation(List *tables, const string &to_find) {
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
			if (StringUtil::CIEquals(table_name, to_find)) {
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

unique_ptr<TableRef>
PostgresReplacementScan(ClientContext &context, const string &table_name, ReplacementScanData *data) {
	auto &scan_data = reinterpret_cast<PostgresReplacementScanData &>(*data);
	// Use 'QueryDesc *desc' to query the postgres table
	// We will return a custom table function scan with parameters (likely passing a pointer as parameter)

	auto tables = scan_data.desc->plannedstmt->rtable;
	auto table = FindMatchingRelation(tables, table_name);
	if (!table) {
		elog(ERROR, "Failed to find table %s in replacement scan lookup", table_name.c_str());
		return nullptr;
	}

	// Then inside the table function we can scan tuples from the postgres table and convert them into duckdb vectors.
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	// table = POINTER(table)
	children.push_back(
	    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>("table"),
	                                    make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(table)))));
	// snapshot = POINTER(snapshot)
	children.push_back(make_uniq<ComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>("snapshot"),
	    make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(scan_data.desc->estate->es_snapshot)))));
	table_function->function = make_uniq<FunctionExpression>("postgres_scan", std::move(children));
	return std::move(table_function);
}

} // namespace duckdb
