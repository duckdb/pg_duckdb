#include "duckdb.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/types/uuid.hpp"

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "utils/numeric.h"
#include "utils/uuid.h"
#include "utils/array.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
}

#include "quack/types/decimal.hpp"
#include "quack/quack.h"
#include "quack/quack_filter.hpp"
#include "quack/quack_heap_seq_scan.hpp"
#include "quack/quack_detoast.hpp"
#include "quack/quack_types.hpp"

namespace quack {

struct CharArray {
public:
	static ArrayType *ConstructArray(Datum *datums, bool *nulls, int *count, int *lower_bound) {
		return construct_md_array(datums, nulls, 1, count, lower_bound, CHAROID, 1, true, 'c');
	}
	static duckdb::LogicalTypeId ExpectedType() {
		return duckdb::LogicalTypeId::TINYINT;
	}
	static Datum ConvertToPostgres(const duckdb::Value &val) {
		return Datum(val.GetValue<int8_t>());
	}
};

struct BoolArray {
public:
	static ArrayType *ConstructArray(Datum *datums, bool *nulls, int *count, int *lower_bound) {
		return construct_md_array(datums, nulls, 1, count, lower_bound, BOOLOID, sizeof(bool), true, 'c');
	}
	static duckdb::LogicalTypeId ExpectedType() {
		return duckdb::LogicalTypeId::BOOLEAN;
	}
	static Datum ConvertToPostgres(const duckdb::Value &val) {
		return Int32GetDatum(val.GetValue<bool>());
	}
};

struct Int4Array {
public:
	static ArrayType *ConstructArray(Datum *datums, bool *nulls, int *count, int *lower_bound) {
		return construct_md_array(datums, nulls, 1, count, lower_bound, INT4OID, sizeof(int32_t), true, 'i');
	}
	static duckdb::LogicalTypeId ExpectedType() {
		return duckdb::LogicalTypeId::INTEGER;
	}
	static Datum ConvertToPostgres(const duckdb::Value &val) {
		return Int32GetDatum(val.GetValue<int32_t>());
	}
};

static void ConvertDouble(TupleTableSlot *slot, double value, idx_t col) {
	slot->tts_tupleDescriptor->attrs[col].atttypid = FLOAT8OID;
	slot->tts_tupleDescriptor->attrs[col].attbyval = true;
	memcpy(&slot->tts_values[col], (char *)&value, sizeof(double));
}

template <class T, class OP = DecimalConversionInteger>
NumericVar ConvertNumeric(T value, idx_t scale) {
	NumericVar result;
	auto &sign = result.sign;
	result.dscale = scale;
	auto &weight = result.weight;
	auto &ndigits = result.ndigits;

	constexpr idx_t MAX_DIGITS = sizeof(T) * 4;
	if (value < 0) {
		value = -value;
		sign = NUMERIC_NEG;
	} else {
		sign = NUMERIC_POS;
	}
	// divide the decimal into the integer part (before the decimal point) and fractional part (after the point)
	T integer_part;
	T fractional_part;
	if (scale == 0) {
		integer_part = value;
		fractional_part = 0;
	} else {
		integer_part = value / OP::GetPowerOfTen(scale);
		fractional_part = value % OP::GetPowerOfTen(scale);
	}

	uint16_t integral_digits[MAX_DIGITS];
	uint16_t fractional_digits[MAX_DIGITS];
	int32_t integral_ndigits;

	// split the integral part into parts of up to NBASE (4 digits => 0..9999)
	integral_ndigits = 0;
	while (integer_part > 0) {
		integral_digits[integral_ndigits++] = uint16_t(integer_part % T(NBASE));
		integer_part /= T(NBASE);
	}
	weight = integral_ndigits - 1;
	// split the fractional part into parts of up to NBASE (4 digits => 0..9999)
	// count the amount of digits required for the fractional part
	// note that while it is technically possible to leave out zeros here this adds even more complications
	// so we just always write digits for the full "scale", even if not strictly required
	int32_t fractional_ndigits = (scale + DEC_DIGITS - 1) / DEC_DIGITS;
	// fractional digits are LEFT aligned (for some unknown reason)
	// that means if we write ".12" with a scale of 2 we actually need to write "1200", instead of "12"
	// this means we need to "correct" the number 12 by multiplying by 100 in this case
	// this correction factor is the "number of digits to the next full number"
	int32_t correction = fractional_ndigits * DEC_DIGITS - scale;
	fractional_part *= OP::GetPowerOfTen(correction);
	for (idx_t i = 0; i < fractional_ndigits; i++) {
		fractional_digits[i] = uint16_t(fractional_part % NBASE);
		fractional_part /= NBASE;
	}

	ndigits = integral_ndigits + fractional_ndigits;

	result.buf = (NumericDigit *)palloc(ndigits * sizeof(NumericDigit));
	result.digits = result.buf;
	auto &digits = result.digits;

	idx_t digits_idx = 0;
	for (idx_t i = integral_ndigits; i > 0; i--) {
		digits[digits_idx++] = integral_digits[i - 1];
	}
	for (idx_t i = fractional_ndigits; i > 0; i--) {
		digits[digits_idx++] = fractional_digits[i - 1];
	}

	return result;
}

template <class OP>
static void ConvertDuckToPostgresArray(TupleTableSlot *slot, duckdb::Value &value, idx_t col) {
	D_ASSERT(value.type().id() == duckdb::LogicalTypeId::LIST);
	auto child_type = duckdb::ListType::GetChildType(value.type());
	auto child_id = child_type.id();
	D_ASSERT(child_id == OP::ExpectedType());
	(void)child_id;

	auto values = duckdb::ListValue::GetChildren(value);
	int count = values.size();

	// Allocate memory for Datum array and nulls array
	auto datums = (Datum *) palloc(count * sizeof(Datum));
	auto nulls = (bool *) palloc(count * sizeof(bool));

	// Fill the Datum and nulls arrays
	for (idx_t i = 0; i < count; i++) {
		auto &child_val = values[i];
		nulls[i] = child_val.IsNull();
		if (!nulls[i]) {
			datums[i] = OP::ConvertToPostgres(values[i]);
		}
	}

	// Create the array
	int lower_bound = 1;
	auto arr = OP::ConstructArray(datums, nulls, &count, &lower_bound);

	// Free allocated memory
	pfree(datums);
	pfree(nulls);

	slot->tts_values[col] = PointerGetDatum(arr);
}

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
	case JSONOID:
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
		slot->tts_values[col] = date.days - quack::QUACK_DUCK_DATE_OFFSET;
		break;
	}
	case TIMESTAMPOID: {
		duckdb::timestamp_t timestamp = value.GetValue<duckdb::timestamp_t>();
		slot->tts_values[col] = timestamp.value - quack::QUACK_DUCK_TIMESTAMP_OFFSET;
		break;
	}
	case FLOAT4OID: {
		auto result_float = value.GetValue<float>();
		slot->tts_tupleDescriptor->attrs[col].atttypid = FLOAT4OID;
		slot->tts_tupleDescriptor->attrs[col].attbyval = true;
		memcpy(&slot->tts_values[col], (char *)&result_float, sizeof(float));
		break;
	}
	case FLOAT8OID: {
		double result_double = value.GetValue<double>();
		ConvertDouble(slot, result_double, col);
		break;
	}
	case NUMERICOID: {
		if (value.type().id() == duckdb::LogicalTypeId::DOUBLE) {
			auto result_double = value.GetValue<double>();
			ConvertDouble(slot, result_double, col);
			break;
		}
		NumericVar numeric_var;
		D_ASSERT(value.type().id() == duckdb::LogicalTypeId::DECIMAL);
		auto physical_type = value.type().InternalType();
		auto scale = duckdb::DecimalType::GetScale(value.type());
		switch (physical_type) {
			case duckdb::PhysicalType::INT16: {
				elog(INFO, "SMALLINT");
				numeric_var = ConvertNumeric<int16_t>(value.GetValueUnsafe<int16_t>(), scale);
				break;
			}
			case duckdb::PhysicalType::INT32: {
				elog(INFO, "INTEGER");
				numeric_var = ConvertNumeric<int32_t>(value.GetValueUnsafe<int32_t>(), scale);
				break;
			}
			case duckdb::PhysicalType::INT64: {
				elog(INFO, "BIGINT");
				numeric_var = ConvertNumeric<int64_t>(value.GetValueUnsafe<int64_t>(), scale);
				break;
			}
			case duckdb::PhysicalType::INT128: {
				elog(INFO, "HUGEINT");
				numeric_var = ConvertNumeric<hugeint_t, DecimalConversionHugeint>(value.GetValueUnsafe<hugeint_t>(), scale);
				break;
			}
			default: {
				elog(ERROR, "Unrecognized physical type for DECIMAL value");
				break;
			}
		}
		auto numeric = CreateNumeric(numeric_var, NULL);
		auto datum = NumericGetDatum(numeric);
		slot->tts_values[col] = datum;
		break;
	}
	case UUIDOID: {
		D_ASSERT(value.type().id() == duckdb::LogicalTypeId::UUID);
		D_ASSERT(value.type().InternalType() == duckdb::PhysicalType::INT128);
		auto duckdb_uuid = value.GetValue<hugeint_t>();
		pg_uuid_t *postgres_uuid = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));

		duckdb_uuid.upper ^= (uint64_t(1) << 63);
		// Convert duckdb_uuid to bytes and store in postgres_uuid.data
		uint8_t *uuid_bytes = (uint8_t *)&duckdb_uuid;

		for (int i = 0; i < UUID_LEN; ++i) {
			postgres_uuid->data[i] = uuid_bytes[UUID_LEN - 1 - i];
		}

		auto datum = UUIDPGetDatum(postgres_uuid);
		slot->tts_values[col] = datum;
		break;
	}
	case BPCHARARRAYOID:
	case CHARARRAYOID: {
		ConvertDuckToPostgresArray<CharArray>(slot, value, col);
		break;
	}
	case BOOLARRAYOID: {
		ConvertDuckToPostgresArray<BoolArray>(slot, value, col);
		break;
	}
	case INT4ARRAYOID: {
		ConvertDuckToPostgresArray<Int4Array>(slot, value, col);
		break;
	}
	default:
		elog(ERROR, "(DuckDB/ConvertDuckToPostgresValue) Unsuported quack type: %d", oid);
	}
}

static inline int
numeric_typmod_precision(int32 typmod)
{
	return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

static inline int
numeric_typmod_scale(int32 typmod)
{
	return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}

duckdb::LogicalType
ConvertPostgresToDuckColumnType(Oid type, int32_t typmod) {
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
	case FLOAT4OID:
		return duckdb::LogicalTypeId::FLOAT;
	case FLOAT8OID:
		return duckdb::LogicalTypeId::DOUBLE;
	case NUMERICOID: {
		auto precision = numeric_typmod_precision(typmod);
		auto scale = numeric_typmod_scale(typmod);
		if (typmod == -1 || precision < 0 || scale < 0 || precision > 38) {
			auto extra_type_info = duckdb::make_shared<NumericAsDouble>();
			return duckdb::LogicalType(duckdb::LogicalTypeId::DOUBLE, std::move(extra_type_info));
		}
		return duckdb::LogicalType::DECIMAL(precision, scale);
	}
	case UUIDOID:
		return duckdb::LogicalTypeId::UUID;
	case JSONOID:
		return duckdb::LogicalType::JSON();
	case BPCHARARRAYOID: {
		auto extra_type_info = duckdb::make_shared<IsBpChar>();
		auto child_type = duckdb::LogicalType(duckdb::LogicalTypeId::TINYINT, std::move(extra_type_info));
		return duckdb::LogicalType::LIST(std::move(child_type));
	}
	case CHARARRAYOID:
		return duckdb::LogicalType::LIST(duckdb::LogicalTypeId::TINYINT);
	case BOOLARRAYOID:
		return duckdb::LogicalType::LIST(duckdb::LogicalTypeId::BOOLEAN);
	case INT4ARRAYOID:
		return duckdb::LogicalType::LIST(duckdb::LogicalTypeId::INTEGER);
	default:
		elog(ERROR, "(DuckDB/ConvertPostgresToDuckColumnType) Unsupported quack type: %d", type);
	}
}

Oid
GetPostgresDuckDBType(duckdb::LogicalType type) {
	auto id = type.id();
	switch (id) {
	case duckdb::LogicalTypeId::BOOLEAN:
		return BOOLOID;
	case duckdb::LogicalTypeId::TINYINT:
		return CHAROID;
	case duckdb::LogicalTypeId::SMALLINT:
		return INT2OID;
	case duckdb::LogicalTypeId::INTEGER:
		return INT4OID;
	case duckdb::LogicalTypeId::BIGINT:
		return INT8OID;
	case duckdb::LogicalTypeId::HUGEINT:
		return NUMERICOID;
	case duckdb::LogicalTypeId::VARCHAR: {
		if (type.IsJSONType()) {
			return JSONOID;
		}
		return VARCHAROID;
	}
	case duckdb::LogicalTypeId::DATE:
		return DATEOID;
	case duckdb::LogicalTypeId::TIMESTAMP:
		return TIMESTAMPOID;
	case duckdb::LogicalTypeId::FLOAT:
		return FLOAT4OID;
	case duckdb::LogicalTypeId::DOUBLE:
		return FLOAT8OID;
	case duckdb::LogicalTypeId::DECIMAL: {
		return NUMERICOID;
	}
	case duckdb::LogicalTypeId::UUID:
		return UUIDOID;
	case duckdb::LogicalTypeId::LIST: {
		auto child_type = duckdb::ListType::GetChildType(type);
		auto child_type_id = child_type.id();
		switch (child_type_id) {
			case duckdb::LogicalTypeId::TINYINT:
				return CHARARRAYOID;
			case duckdb::LogicalTypeId::BOOLEAN:
				return BOOLARRAYOID;
			case duckdb::LogicalTypeId::INTEGER:
				return INT4ARRAYOID;
			default:
				elog(ERROR, "(DuckDB/GetPostgresDuckDBType) Unsupported quack type: %s", type.ToString().c_str());
		}
	}
	default: {
		elog(ERROR, "(DuckDB/GetPostgresDuckDBType) Unsupported quack type: %s", type.ToString().c_str());
		break;
	}
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

static bool NumericIsNegative(const NumericVar &numeric) {
	return numeric.sign == NUMERIC_NEG;
}

template <class T, class OP = DecimalConversionInteger>
T ConvertDecimal(const NumericVar &numeric) {
	auto scale_POWER = OP::GetPowerOfTen(numeric.dscale);

	if (numeric.ndigits == 0) {
		return 0;
	}
	T integral_part = 0, fractional_part = 0;

	if (numeric.weight >= 0) {
		idx_t digit_index = 0;
		integral_part = numeric.digits[digit_index++];
		for (; digit_index <= numeric.weight; digit_index++) {
			integral_part *= NBASE;
			if (digit_index < numeric.ndigits) {
				integral_part += numeric.digits[digit_index];
			}
		}
		integral_part *= scale_POWER;
	}

	// we need to find out how large the fractional part is in terms of powers
	// of ten this depends on how many times we multiplied with NBASE
	// if that is different from scale, we need to divide the extra part away
	// again
	// similarly, if trailing zeroes have been suppressed, we have not been multiplying t
	// the fractional part with NBASE often enough. If so, add additional powers
	if (numeric.ndigits > numeric.weight + 1) {
		auto fractional_power = (numeric.ndigits - numeric.weight - 1) * DEC_DIGITS;
		auto fractional_power_correction = fractional_power - numeric.dscale;
		D_ASSERT(fractional_power_correction < 20);
		fractional_part = 0;
		for (int32_t i = duckdb::MaxValue<int32_t>(0, numeric.weight + 1); i < numeric.ndigits; i++) {
			if (i + 1 < numeric.ndigits) {
				// more digits remain - no need to compensate yet
				fractional_part *= NBASE;
				fractional_part += numeric.digits[i];
			} else {
				// last digit, compensate
				T final_base = NBASE;
				T final_digit = numeric.digits[i];
				if (fractional_power_correction >= 0) {
					T compensation = OP::GetPowerOfTen(fractional_power_correction);
					final_base /= compensation;
					final_digit /= compensation;
				} else {
					T compensation = OP::GetPowerOfTen(-fractional_power_correction);
					final_base *= compensation;
					final_digit *= compensation;
				}
				fractional_part *= final_base;
				fractional_part += final_digit;
			}
		}
	}

	// finally
	auto base_res = OP::Finalize(numeric, integral_part + fractional_part);
	return (NumericIsNegative(numeric) ? -base_res : base_res);
}

void
ConvertPostgresToDuckValue(Datum value, duckdb::Vector &result, idx_t offset) {
	auto &type = result.GetType();
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		Append<bool>(result, DatumGetBool(value), offset);
		break;
	case duckdb::LogicalTypeId::TINYINT: {
		auto aux_info = type.GetAuxInfoShrPtr();
		if (aux_info && dynamic_cast<IsBpChar *>(aux_info.get())) {
			auto res = DatumGetBpCharPP(value);
			auto bpchar_length = VARSIZE_ANY_EXHDR(value);
			auto bpchar_data = VARDATA_ANY(value);

			if (bpchar_length != 1) {
				elog(ERROR, "Expected 1 length BPCHAR for TINYINT marked with IsBpChar at offset %d", offset);
			}
			Append<int8_t>(result, bpchar_data[0], offset);
		} else {
			Append<int8_t>(result, DatumGetChar(value), offset);
		}
		break;
	}
	case duckdb::LogicalTypeId::SMALLINT:
		Append<int16_t>(result, DatumGetInt16(value), offset);
		break;
	case duckdb::LogicalTypeId::INTEGER:
		Append<int32_t>(result, DatumGetInt32(value), offset);
		break;
	case duckdb::LogicalTypeId::BIGINT:
		Append<int64_t>(result, DatumGetInt64(value), offset);
		break;
	case duckdb::LogicalTypeId::VARCHAR: {
		// NOTE: This also handles JSON
		AppendString(result, value, offset);
		break;
	}
	case duckdb::LogicalTypeId::DATE:
		Append<duckdb::date_t>(result, duckdb::date_t(static_cast<int32_t>(value + QUACK_DUCK_DATE_OFFSET)), offset);
		break;
	case duckdb::LogicalTypeId::TIMESTAMP:
		Append<duckdb::timestamp_t>(result, duckdb::timestamp_t(static_cast<int64_t>(value + QUACK_DUCK_TIMESTAMP_OFFSET)),
								offset);
		break;
	case duckdb::LogicalTypeId::FLOAT: {
		Append<float>(result, DatumGetFloat4(value), offset);
		break;
	}
	case duckdb::LogicalTypeId::DOUBLE: {
		auto aux_info = type.GetAuxInfoShrPtr();
		if (aux_info && dynamic_cast<NumericAsDouble *>(aux_info.get())) {
			// This NUMERIC could not be converted to a DECIMAL, convert it as DOUBLE instead
			auto numeric = DatumGetNumeric(value);
			auto numeric_var = FromNumeric(numeric);
			auto double_val = ConvertDecimal<double, DecimalConversionDouble>(numeric_var);
			Append<double>(result, double_val, offset);
		} else {
			Append<double>(result, DatumGetFloat8(value), offset);
		}
		break;
	}
	case duckdb::LogicalTypeId::DECIMAL: {
		auto physical_type = type.InternalType();
		auto numeric = DatumGetNumeric(value);
		auto numeric_var = FromNumeric(numeric);
		switch (physical_type) {
			case duckdb::PhysicalType::INT16: {
				Append(result, ConvertDecimal<int16_t>(numeric_var), offset);
				break;
			}
			case duckdb::PhysicalType::INT32: {
				Append(result, ConvertDecimal<int32_t>(numeric_var), offset);
				break;
			}
			case duckdb::PhysicalType::INT64: {
				Append(result, ConvertDecimal<int64_t>(numeric_var), offset);
				break;
			}
			case duckdb::PhysicalType::INT128: {
				Append(result, ConvertDecimal<hugeint_t, DecimalConversionHugeint>(numeric_var), offset);
				break;
			}
			default: {
				elog(ERROR, "Unrecognized physical type for DECIMAL value");
				break;
			}
		}
		break;
	}
	case duckdb::LogicalTypeId::UUID: {
		auto uuid = DatumGetPointer(value);
		hugeint_t duckdb_uuid;
		D_ASSERT(UUID_LEN == sizeof(hugeint_t));
		for (idx_t i = 0; i < UUID_LEN; i++) {
			((uint8_t*)&duckdb_uuid)[UUID_LEN-1-i] = ((uint8_t*)uuid)[i];
		}
		duckdb_uuid.upper ^= (uint64_t(1) << 63);
		Append(result, duckdb_uuid, offset);
		break;
	}
	case duckdb::LogicalTypeId::LIST: {
		auto &child = duckdb::ListVector::GetEntry(result);

		// Convert Datum to ArrayType
		auto array = DatumGetArrayTypeP(value);

		int16 typlen;
		bool typbyval;
		char typalign;
		get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);

		int nelems;
		Datum *elems;
		bool *nulls;
		// Deconstruct the array into Datum elements
		deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elems, &nulls, &nelems);

		auto list_data = duckdb::FlatVector::GetData<duckdb::list_entry_t>(result);
		auto child_offset = duckdb::ListVector::GetListSize(result);
		list_data[offset] = duckdb::list_entry_t(
			child_offset,
			nelems
		);

		auto total_elems = child_offset + nelems;
		duckdb::ListVector::Reserve(result, total_elems);
		duckdb::ListVector::SetListSize(result, total_elems);

		auto child_type = child.GetType();
		auto child_id = child_type.id();
		switch (child_id) {
			case duckdb::LogicalTypeId::TINYINT:
			case duckdb::LogicalTypeId::BOOLEAN:
			case duckdb::LogicalTypeId::INTEGER: {
				for (int i = 0; i < nelems; i++) {
					idx_t dest_idx = child_offset + i;
					if (nulls[i]) {
						auto &array_mask = duckdb::FlatVector::Validity(child);
						array_mask.SetInvalid(dest_idx);
						continue;
					}
					ConvertPostgresToDuckValue(elems[i], child, dest_idx);
				}
				break;
			}
			default:
				elog(ERROR, "(DuckDB/ConvertPostgresToDuckValue) Unsupported quack type: %s", result.GetType().ToString().c_str());
		}
		break;
	}
	default:
		elog(ERROR, "(DuckDB/ConvertPostgresToDuckValue) Unsupported quack type: %s", result.GetType().ToString().c_str());
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

	if (parallelScanState.m_count_tuples_only) {
		threadScanInfo.m_output_vector_size++;
		return;
	}

	/* FIXME: all calls to duckdb_malloc/duckdb_free should be changed in future */
	Datum *values = (Datum *)duckdb_malloc(sizeof(Datum) * parallelScanState.m_columns.size());
	bool *nulls = (bool *)duckdb_malloc(sizeof(bool) * parallelScanState.m_columns.size());

	bool validTuple = true;

	for (auto const &[columnIdx, valueIdx] : parallelScanState.m_columns) {
		values[valueIdx] = HeapTupleFetchNextColumnDatum(threadScanInfo.m_tuple_desc, &threadScanInfo.m_tuple,
														 heapTupleReadState, columnIdx + 1, &nulls[valueIdx]);
		if (parallelScanState.m_filters &&
			(parallelScanState.m_filters->filters.find(valueIdx) != parallelScanState.m_filters->filters.end())) {
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
			idx_t projectionColumnIdx = parallelScanState.m_columns[parallelScanState.m_projections[idx]];
			if (threadScanInfo.m_tuple_desc->attrs[parallelScanState.m_projections[idx]].attlen == -1) {
				bool shouldFree = false;
				values[projectionColumnIdx] =
					DetoastPostgresDatum(reinterpret_cast<varlena *>(values[projectionColumnIdx]),
										 parallelScanState.m_lock, &shouldFree);
				ConvertPostgresToDuckValue(values[projectionColumnIdx], result, threadScanInfo.m_output_vector_size);
				if (shouldFree) {
					duckdb_free(reinterpret_cast<void *>(values[projectionColumnIdx]));
				}
			} else {
				ConvertPostgresToDuckValue(values[projectionColumnIdx], result, threadScanInfo.m_output_vector_size);
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
