#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
}

#include "quack/quack.h"
#include "quack/quack_filter.hpp"
#include "quack/quack_heap_seq_scan.hpp"
#include "quack/quack_detoast.hpp"

namespace quack {

// DuckDB has date starting from 1/1/1970 while PG starts from 1/1/2000
constexpr int32_t QUACK_DUCK_DATE_OFFSET = 10957;
constexpr int64_t QUACK_DUCK_TIMESTAMP_OFFSET = INT64CONST(10957) * USECS_PER_DAY;

void
ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, idx_t col) {
	Oid oid = slot->tts_tupleDescriptor->attrs[col].atttypid;

	switch (oid) {
	case BOOLOID:
		slot->tts_values[col] = value.GetValue<bool>();
		break;
	case CHAROID:
		slot->tts_values[col] = value.GetValue<int8_t>();
		break;
	case INT2OID:
		slot->tts_values[col] = value.GetValue<int16_t>();
		break;
	case INT4OID:
		slot->tts_values[col] = value.GetValue<int32_t>();
		break;
	case INT8OID:
		slot->tts_values[col] = value.GetValue<int64_t>();
		break;
	case BPCHAROID:
	case TEXTOID:
	case VARCHAROID: {
		auto str = value.GetValue<duckdb::string>();
		auto varchar = str.c_str();
		auto varchar_len = str.size();

		text *result = (text *)palloc0(varchar_len + VARHDRSZ);
		SET_VARSIZE(result, varchar_len + VARHDRSZ);
		memcpy(VARDATA(result), varchar, varchar_len);
		slot->tts_values[col] = PointerGetDatum(result);
		break;
	}
	case DATEOID: {
		duckdb::date_t date = value.GetValue<duckdb::date_t>();
		slot->tts_values[col] = date.days - QUACK_DUCK_DATE_OFFSET;
		break;
	}
	case TIMESTAMPOID: {
		duckdb::dtime_t timestamp = value.GetValue<duckdb::dtime_t>();
		slot->tts_values[col] = timestamp.micros - QUACK_DUCK_TIMESTAMP_OFFSET;
		break;
	}
	case FLOAT8OID:
	case NUMERICOID: {
		double result_double = value.GetValue<double>();
		slot->tts_tupleDescriptor->attrs[col].atttypid = FLOAT8OID;
		slot->tts_tupleDescriptor->attrs[col].attbyval = true;
		memcpy(&slot->tts_values[col], (char *)&result_double, sizeof(double));
		break;
	}
	default:
		elog(ERROR, "Unsuported quack type: %d", oid);
	}
}

duckdb::LogicalType
ConvertPostgresToDuckColumnType(Oid type) {
	switch (type) {
	case BOOLOID:
		return duckdb::LogicalTypeId::BOOLEAN;
	case CHAROID:
		return duckdb::LogicalTypeId::TINYINT;
	case INT2OID:
		return duckdb::LogicalTypeId::SMALLINT;
	case INT4OID:
		return duckdb::LogicalTypeId::INTEGER;
	case INT8OID:
		return duckdb::LogicalTypeId::BIGINT;
	case BPCHAROID:
	case TEXTOID:
	case VARCHAROID:
		return duckdb::LogicalTypeId::VARCHAR;
	case DATEOID:
		return duckdb::LogicalTypeId::DATE;
	case TIMESTAMPOID:
		return duckdb::LogicalTypeId::TIMESTAMP;
	default:
		elog(ERROR, "Unsupported quack type: %d", type);
	}
}

template <class T>
static void
Append(duckdb::Vector &result, T value, idx_t offset) {
	auto data = duckdb::FlatVector::GetData<T>(result);
	data[offset] = value;
}

static void
AppendString(duckdb::Vector &result, Datum value, idx_t offset) {
	const char *text = VARDATA_ANY(value);
	int len = VARSIZE_ANY_EXHDR(value);
	duckdb::string_t str(text, len);

	auto data = duckdb::FlatVector::GetData<duckdb::string_t>(result);
	data[offset] = duckdb::StringVector::AddString(result, str);
}

void
ConvertPostgresToDuckValue(Datum value, duckdb::Vector &result, idx_t offset) {
	switch (result.GetType().id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		Append<bool>(result, DatumGetBool(value), offset);
		break;
	case duckdb::LogicalTypeId::TINYINT:
		Append<int8_t>(result, DatumGetChar(value), offset);
		break;
	case duckdb::LogicalTypeId::SMALLINT:
		Append<int16_t>(result, DatumGetInt16(value), offset);
		break;
	case duckdb::LogicalTypeId::INTEGER:
		Append<int32_t>(result, DatumGetInt32(value), offset);
		break;
	case duckdb::LogicalTypeId::BIGINT:
		Append<int64_t>(result, DatumGetInt64(value), offset);
		break;
	case duckdb::LogicalTypeId::VARCHAR:
		AppendString(result, value, offset);
		break;
	case duckdb::LogicalTypeId::DATE:
		Append<duckdb::date_t>(result, duckdb::date_t(static_cast<int32_t>(value + QUACK_DUCK_DATE_OFFSET)), offset);
		break;
	case duckdb::LogicalTypeId::TIMESTAMP:
		Append<duckdb::dtime_t>(result, duckdb::dtime_t(static_cast<int64_t>(value + QUACK_DUCK_TIMESTAMP_OFFSET)),
		                        offset);
		break;
	default:
		elog(ERROR, "Unsupported quack type: %d", static_cast<int>(result.GetType().id()));
		break;
	}
}

typedef struct HeapTupleReadState {
	bool m_slow = 0;
	int m_last_tuple_att = 0;
	uint32 m_page_tuple_offset = 0;
} HeapTupleReadState;

static Datum
HeapTupleFetchNextColumnDatum(TupleDesc tupleDesc, HeapTuple tuple, HeapTupleReadState &heapTupleReadState, int attNum,
                              bool *isNull) {

	HeapTupleHeader tup = tuple->t_data;
	bool hasnulls = HeapTupleHasNulls(tuple);
	int attnum;
	char *tp;
	uint32 off;
	bits8 *bp = tup->t_bits;
	bool slow = false;
	Datum value = (Datum)0;

	attnum = heapTupleReadState.m_last_tuple_att;

	if (attnum == 0) {
		/* Start from the first attribute */
		off = 0;
		heapTupleReadState.m_slow = false;
	} else {
		/* Restore state from previous execution */
		off = heapTupleReadState.m_page_tuple_offset;
		slow = heapTupleReadState.m_slow;
	}

	tp = (char *)tup + tup->t_hoff;

	for (; attnum < attNum; attnum++) {
		Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);

		if (hasnulls && att_isnull(attnum, bp)) {
			value = (Datum)0;
			*isNull = true;
			slow = true; /* can't use attcacheoff anymore */
			continue;
		}

		*isNull = false;

		if (!slow && thisatt->attcacheoff >= 0) {
			off = thisatt->attcacheoff;
		} else if (thisatt->attlen == -1) {
			if (!slow && off == att_align_nominal(off, thisatt->attalign)) {
				thisatt->attcacheoff = off;
			} else {
				off = att_align_pointer(off, thisatt->attalign, -1, tp + off);
				slow = true;
			}
		} else {
			off = att_align_nominal(off, thisatt->attalign);
			if (!slow) {
				thisatt->attcacheoff = off;
			}
		}

		value = fetchatt(thisatt, tp + off);

		off = att_addlength_pointer(off, thisatt->attlen, tp + off);

		if (thisatt->attlen <= 0) {
			slow = true;
		}
	}

	heapTupleReadState.m_last_tuple_att = attNum;
	heapTupleReadState.m_page_tuple_offset = off;

	if (slow) {
		heapTupleReadState.m_slow = true;
	} else {
		heapTupleReadState.m_slow = false;
	}

	return value;
}

void
InsertTupleIntoChunk(duckdb::DataChunk &output, PostgresHeapSeqScanThreadInfo &threadScanInfo,
                     PostgresHeapSeqParallelScanState &parallelScanState) {
	HeapTupleReadState heapTupleReadState = {};

	if (parallelScanState.m_count_tuple_only) {
		threadScanInfo.m_output_vector_size++;
		return;
	}

	Datum *values = (Datum *)duckdb_malloc(sizeof(Datum) * parallelScanState.m_columns.size());
	bool *nulls = (bool *)duckdb_malloc(sizeof(bool) * parallelScanState.m_columns.size());

	bool validTuple = true;

	for (auto const &[columnIdx, valueIdx] : parallelScanState.m_columns) {
		values[valueIdx] = HeapTupleFetchNextColumnDatum(threadScanInfo.m_tuple_desc, &threadScanInfo.m_tuple,
		                                                 heapTupleReadState, columnIdx + 1, &nulls[valueIdx]);
		if (parallelScanState.m_filters &&
		    (parallelScanState.m_filters->filters.find(columnIdx) != parallelScanState.m_filters->filters.end())) {
			auto &filter = parallelScanState.m_filters->filters[valueIdx];
			validTuple = ApplyValueFilter(*filter, values[valueIdx], nulls[valueIdx],
			                              threadScanInfo.m_tuple_desc->attrs[columnIdx].atttypid);
		}

		if (!validTuple) {
			break;
		}
	}

	for (idx_t idx = 0; validTuple && idx < parallelScanState.m_projections.size(); idx++) {
		auto &result = output.data[idx];
		if (nulls[idx]) {
			auto &array_mask = duckdb::FlatVector::Validity(result);
			array_mask.SetInvalid(threadScanInfo.m_output_vector_size);
		} else {
			if (threadScanInfo.m_tuple_desc->attrs[parallelScanState.m_projections[idx]].attlen == -1) {
				bool shouldFree = false;
				values[idx] = DetoastPostgresDatum(reinterpret_cast<varlena *>(values[idx]), parallelScanState.m_lock,
				                                   &shouldFree);
				ConvertPostgresToDuckValue(values[idx], result, threadScanInfo.m_output_vector_size);
				if (shouldFree) {
					duckdb_free(reinterpret_cast<void *>(values[idx]));
				}
			} else {
				ConvertPostgresToDuckValue(values[idx], result, threadScanInfo.m_output_vector_size);
			}
		}
	}

	if (validTuple) {
		threadScanInfo.m_output_vector_size++;
	}

	parallelScanState.m_total_row_count++;

	duckdb_free(values);
	duckdb_free(nulls);
}

} // namespace quack
