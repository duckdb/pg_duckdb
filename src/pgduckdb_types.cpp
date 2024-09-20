#include "duckdb.hpp"
#include "duckdb/common/shared_ptr.hpp"
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
#include "utils/syscache.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/types/decimal.hpp"
#include "pgduckdb/pgduckdb_filter.hpp"
#include "pgduckdb/pgduckdb_detoast.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

namespace pgduckdb {

struct BoolArray {
public:
	static ArrayType *
	ConstructArray(Datum *datums, bool *nulls, int ndims, int *dims, int *lower_bound) {
		return construct_md_array(datums, nulls, ndims, dims, lower_bound, BOOLOID, sizeof(bool), true, 'c');
	}
	static duckdb::LogicalTypeId
	ExpectedType() {
		return duckdb::LogicalTypeId::BOOLEAN;
	}
	static Datum
	ConvertToPostgres(const duckdb::Value &val) {
		return Datum(val.GetValue<bool>());
	}
};

template <int32_t OID>
struct PostgresIntegerOIDMapping {};

template <>
struct PostgresIntegerOIDMapping<CHAROID> {
	static constexpr int32_t postgres_oid = CHAROID;
	using physical_type = int8_t;
	static constexpr duckdb::LogicalTypeId duck_type_id = duckdb::LogicalTypeId::TINYINT;
	static Datum
	ToDatum(const duckdb::Value &val) {
		return Datum(val.GetValue<physical_type>());
	}
};

template <>
struct PostgresIntegerOIDMapping<INT2OID> {
	static constexpr int32_t postgres_oid = INT2OID;
	using physical_type = int16_t;
	static constexpr duckdb::LogicalTypeId duck_type_id = duckdb::LogicalTypeId::SMALLINT;
	static Datum
	ToDatum(const duckdb::Value &val) {
		return Int16GetDatum(val.GetValue<physical_type>());
	}
};

template <>
struct PostgresIntegerOIDMapping<INT4OID> {
	static constexpr int32_t postgres_oid = INT4OID;
	using physical_type = int32_t;
	static constexpr duckdb::LogicalTypeId duck_type_id = duckdb::LogicalTypeId::INTEGER;
	static Datum
	ToDatum(const duckdb::Value &val) {
		return Int32GetDatum(val.GetValue<physical_type>());
	}
};

template <>
struct PostgresIntegerOIDMapping<INT8OID> {
	static constexpr int32_t postgres_oid = INT8OID;
	using physical_type = int64_t;
	static constexpr duckdb::LogicalTypeId duck_type_id = duckdb::LogicalTypeId::BIGINT;
	static Datum
	ToDatum(const duckdb::Value &val) {
		return Int64GetDatum(val.GetValue<physical_type>());
	}
};

template <class MAPPING>
struct PODArray {
	using physical_type = typename MAPPING::physical_type;

public:
	static ArrayType *
	ConstructArray(Datum *datums, bool *nulls, int ndims, int *dims, int *lower_bound) {
		return construct_md_array(datums, nulls, ndims, dims, lower_bound, MAPPING::postgres_oid, sizeof(physical_type),
		                          true, 'i');
	}
	static duckdb::LogicalTypeId
	ExpectedType() {
		return MAPPING::duck_type_id;
	}
	static Datum
	ConvertToPostgres(const duckdb::Value &val) {
		return MAPPING::ToDatum(val);
	}
};

static void
ConvertDouble(TupleTableSlot *slot, double value, idx_t col) {
	slot->tts_tupleDescriptor->attrs[col].atttypid = FLOAT8OID;
	slot->tts_tupleDescriptor->attrs[col].attbyval = true;
	memcpy(&slot->tts_values[col], (char *)&value, sizeof(double));
}

template <class T, class OP = DecimalConversionInteger>
NumericVar
ConvertNumeric(T value, idx_t scale) {
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

static const duckdb::LogicalType &
GetChildTypeRecursive(const duckdb::LogicalType &list_type) {
	D_ASSERT(list_type.id() == duckdb::LogicalTypeId::LIST);
	auto &child = duckdb::ListType::GetChildType(list_type);
	if (child.id() == duckdb::LogicalTypeId::LIST) {
		return GetChildTypeRecursive(child);
	}
	return child;
}

static idx_t
GetDuckDBListDimensionality(const duckdb::LogicalType &list_type, idx_t depth = 0) {
	D_ASSERT(list_type.id() == duckdb::LogicalTypeId::LIST);
	auto &child = duckdb::ListType::GetChildType(list_type);
	if (child.id() == duckdb::LogicalTypeId::LIST) {
		return GetDuckDBListDimensionality(child, depth + 1);
	}
	return depth + 1;
}

namespace {

template <class OP>
struct PostgresArrayAppendState {
public:
	PostgresArrayAppendState(idx_t number_of_dimensions) : number_of_dimensions(number_of_dimensions) {
		dimensions = (int *)palloc(number_of_dimensions * sizeof(int));
		lower_bounds = (int *)palloc(number_of_dimensions * sizeof(int));
		for (idx_t i = 0; i < number_of_dimensions; i++) {
			// Initialize everything at -1 to indicate that it isn't set yet
			dimensions[i] = -1;
		}
		for (idx_t i = 0; i < number_of_dimensions; i++) {
			// Lower bounds have no significance for us
			lower_bounds[i] = 1;
		}
	}

public:
	void
	AppendValueAtDimension(const duckdb::Value &value, idx_t dimension) {
		// FIXME: verify that the amount of values does not overflow an `int` ?
		auto &values = duckdb::ListValue::GetChildren(value);
		idx_t to_append = values.size();

		D_ASSERT(dimension < number_of_dimensions);
		if (dimensions[dimension] == -1) {
			// This dimension is not set yet
			dimensions[dimension] = to_append;
			// FIXME: verify that the amount of expected_values does not overflow an `int` ?
			expected_values *= to_append;
		}
		if (dimensions[dimension] != to_append) {
			throw duckdb::InvalidInputException("Expected %d values in list at dimension %d, found %d instead",
			                                    dimensions[dimension], dimension, to_append);
		}

		auto &child_type = duckdb::ListType::GetChildType(value.type());
		if (child_type.id() == duckdb::LogicalTypeId::LIST) {

			for (idx_t i = 0; i < to_append; i++) {
				auto &child_val = values[i];
				if (child_val.IsNull()) {
					// Postgres arrays can not contains nulls at the array level
					// i.e {{1,2}, NULL, {3,4}} is not supported
					throw duckdb::InvalidInputException("Returned LIST contains a NULL at an intermediate dimension "
					                                    "(not the value level), which is not supported in Postgres");
				}
				AppendValueAtDimension(child_val, dimension + 1);
			}
		} else {
			if (!datums) {
				// First time we get to the outer most child
				// Because we traversed all dimensions we know how many values we have to allocate for
				datums = (Datum *)palloc(expected_values * sizeof(Datum));
				nulls = (bool *)palloc(expected_values * sizeof(bool));
			}

			for (idx_t i = 0; i < to_append; i++) {
				auto &child_val = values[i];
				nulls[count + i] = child_val.IsNull();
				if (!nulls[count + i]) {
					datums[count + i] = OP::ConvertToPostgres(values[i]);
				}
			}
			count += to_append;
		}
	}

private:
	idx_t expected_values = 1;
	idx_t count = 0;

public:
	Datum *datums = nullptr;
	bool *nulls = nullptr;
	int *dimensions;
	int *lower_bounds;
	idx_t number_of_dimensions;
};

} // namespace

template <class OP>
static void
ConvertDuckToPostgresArray(TupleTableSlot *slot, duckdb::Value &value, idx_t col) {
	D_ASSERT(value.type().id() == duckdb::LogicalTypeId::LIST);
	auto &child_type = GetChildTypeRecursive(value.type());
	auto child_id = child_type.id();
	D_ASSERT(child_id == OP::ExpectedType());
	(void)child_id;

	auto number_of_dimensions = GetDuckDBListDimensionality(value.type());

	PostgresArrayAppendState<OP> append_state(number_of_dimensions);
	append_state.AppendValueAtDimension(value, 0);

	// Create the array
	auto datums = append_state.datums;
	auto nulls = append_state.nulls;
	auto dimensions = append_state.dimensions;
	auto lower_bounds = append_state.lower_bounds;

	auto arr = OP::ConstructArray(datums, nulls, number_of_dimensions, dimensions, lower_bounds);

	// Free allocated memory
	pfree(datums);
	pfree(nulls);
	pfree(dimensions);
	pfree(lower_bounds);

	slot->tts_values[col] = PointerGetDatum(arr);
}

bool
ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, idx_t col) {
	Oid oid = slot->tts_tupleDescriptor->attrs[col].atttypid;

	switch (oid) {
	case BOOLOID:
		slot->tts_values[col] = value.GetValue<bool>();
		break;
	case CHAROID:
		slot->tts_values[col] = value.GetValue<int8_t>();
		break;
	case INT2OID: {
		if (value.type().id() == duckdb::LogicalTypeId::UTINYINT) {
			slot->tts_values[col] = static_cast<int16_t>(value.GetValue<uint8_t>());
		} else {
			slot->tts_values[col] = value.GetValue<int16_t>();
		}
		break;
	}
	case INT4OID: {
		if (value.type().id() == duckdb::LogicalTypeId::USMALLINT) {
			slot->tts_values[col] = static_cast<int32_t>(value.GetValue<uint16_t>());
		} else {
			slot->tts_values[col] = value.GetValue<int32_t>();
		}
		break;
	}
	case INT8OID: {
		if (value.type().id() == duckdb::LogicalTypeId::UINTEGER) {
			slot->tts_values[col] = static_cast<int64_t>(value.GetValue<uint32_t>());
		} else {
			slot->tts_values[col] = value.GetValue<int64_t>();
		}
		break;
	}
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
		slot->tts_values[col] = date.days - pgduckdb::PGDUCKDB_DUCK_DATE_OFFSET;
		break;
	}
	case TIMESTAMPOID: {
		duckdb::timestamp_t timestamp = value.GetValue<duckdb::timestamp_t>();
		slot->tts_values[col] = timestamp.value - pgduckdb::PGDUCKDB_DUCK_TIMESTAMP_OFFSET;
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
		D_ASSERT(value.type().id() == duckdb::LogicalTypeId::DECIMAL ||
		         value.type().id() == duckdb::LogicalTypeId::HUGEINT);
		auto physical_type = value.type().InternalType();
		const bool is_decimal = value.type().id() == duckdb::LogicalTypeId::DECIMAL;
		uint8_t scale = is_decimal ? duckdb::DecimalType::GetScale(value.type()) : 0;

		switch (physical_type) {
		case duckdb::PhysicalType::INT16: {
			numeric_var = ConvertNumeric<int16_t>(value.GetValueUnsafe<int16_t>(), scale);
			break;
		}
		case duckdb::PhysicalType::INT32: {
			numeric_var = ConvertNumeric<int32_t>(value.GetValueUnsafe<int32_t>(), scale);
			break;
		}
		case duckdb::PhysicalType::INT64: {
			numeric_var = ConvertNumeric<int64_t>(value.GetValueUnsafe<int64_t>(), scale);
			break;
		}
		case duckdb::PhysicalType::INT128: {
			numeric_var = ConvertNumeric<hugeint_t, DecimalConversionHugeint>(value.GetValueUnsafe<hugeint_t>(), scale);
			break;
		}
		default: {
			elog(WARNING, "(PGDuckDB/ConvertDuckToPostgresValue) Unrecognized physical type for DECIMAL value");
			return false;
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
		pg_uuid_t *postgres_uuid = (pg_uuid_t *)palloc(sizeof(pg_uuid_t));

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
	case BOOLARRAYOID: {
		ConvertDuckToPostgresArray<BoolArray>(slot, value, col);
		break;
	}
	case INT4ARRAYOID: {
		ConvertDuckToPostgresArray<PODArray<PostgresIntegerOIDMapping<INT4OID>>>(slot, value, col);
		break;
	}
	case INT8ARRAYOID: {
		ConvertDuckToPostgresArray<PODArray<PostgresIntegerOIDMapping<INT8OID>>>(slot, value, col);
		break;
	}
	default:
		elog(WARNING, "(PGDuckDB/ConvertDuckToPostgresValue) Unsuported pgduckdb type: %d", oid);
		return false;
	}
	return true;
}

static inline int
numeric_typmod_precision(int32 typmod) {
	return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

static inline int
numeric_typmod_scale(int32 typmod) {
	return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}

static duckdb::LogicalType
ChildTypeFromArray(Oid array_type) {
	switch (array_type) {
	case CHARARRAYOID:
		return duckdb::LogicalTypeId::TINYINT;
	case BOOLARRAYOID:
		return duckdb::LogicalTypeId::BOOLEAN;
	case INT4ARRAYOID:
		return duckdb::LogicalTypeId::INTEGER;
	case INT8ARRAYOID:
		return duckdb::LogicalTypeId::BIGINT;
	default:
		throw duckdb::NotImplementedException("No child type set for Postgres OID %d", array_type);
	}
}

duckdb::LogicalType
ConvertPostgresToDuckColumnType(Form_pg_attribute &attribute) {
	auto &type = attribute->atttypid;
	auto &typmod = attribute->atttypmod;
	auto dimensions = attribute->attndims;
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
			auto extra_type_info = duckdb::make_shared_ptr<NumericAsDouble>();
			return duckdb::LogicalType(duckdb::LogicalTypeId::DOUBLE, std::move(extra_type_info));
		}
		return duckdb::LogicalType::DECIMAL(precision, scale);
	}
	case UUIDOID:
		return duckdb::LogicalTypeId::UUID;
	case JSONOID:
		return duckdb::LogicalType::JSON();
	case BOOLARRAYOID:
	case INT4ARRAYOID:
	case INT8ARRAYOID: {
		auto duck_type = ChildTypeFromArray(type);
		for (int i = 0; i < dimensions; i++) {
			duck_type = duckdb::LogicalType::LIST(duck_type);
		}
		return duck_type;
	}
	case REGCLASSOID:
		return duckdb::LogicalTypeId::UINTEGER;
	default: {
		std::string name = "UnsupportedPostgresType (Oid=" + std::to_string(type) + ")";
		return duckdb::LogicalType::USER(name);
	}
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
	case duckdb::LogicalTypeId::UTINYINT:
		return INT2OID;
	case duckdb::LogicalTypeId::USMALLINT:
		return INT4OID;
	case duckdb::LogicalTypeId::UINTEGER:
		return INT8OID;
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
	case duckdb::LogicalTypeId::DECIMAL:
		return NUMERICOID;
	case duckdb::LogicalTypeId::UUID:
		return UUIDOID;
	case duckdb::LogicalTypeId::LIST: {
		const duckdb::LogicalType *duck_type = &type;
		while (duck_type->id() == duckdb::LogicalTypeId::LIST) {
			auto &child_type = duckdb::ListType::GetChildType(*duck_type);
			duck_type = &child_type;
		}
		auto child_type_id = duck_type->id();
		switch (child_type_id) {
		case duckdb::LogicalTypeId::BOOLEAN:
			return BOOLARRAYOID;
		case duckdb::LogicalTypeId::INTEGER:
			return INT4ARRAYOID;
		case duckdb::LogicalTypeId::BIGINT:
			return INT8ARRAYOID;
		default: {
			elog(WARNING, "(PGDuckDB/GetPostgresDuckDBType) Unsupported `LIST` subtype %d to Postgres type",
			     (uint8)child_type_id);
			return InvalidOid;
		}
		}
	}
	default: {
		elog(WARNING, "(PGDuckDB/GetPostgresDuckDBType) Could not convert DuckDB type: %s to Postgres type",
		     type.ToString().c_str());
		return InvalidOid;
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

static bool
NumericIsNegative(const NumericVar &numeric) {
	return numeric.sign == NUMERIC_NEG;
}

template <class T, class OP = DecimalConversionInteger>
T
ConvertDecimal(const NumericVar &numeric) {
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
			auto bpchar_length = VARSIZE_ANY_EXHDR(value);
			auto bpchar_data = VARDATA_ANY(value);

			if (bpchar_length != 1) {
				throw duckdb::InternalException(
				    "Expected 1 length BPCHAR for TINYINT marked with IsBpChar at offset %llu", offset);
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
	case duckdb::LogicalTypeId::UINTEGER:
		Append<uint32_t>(result, DatumGetUInt32(value), offset);
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
		Append<duckdb::date_t>(result, duckdb::date_t(static_cast<int32_t>(value + PGDUCKDB_DUCK_DATE_OFFSET)), offset);
		break;
	case duckdb::LogicalTypeId::TIMESTAMP:
		Append<duckdb::timestamp_t>(
		    result, duckdb::timestamp_t(static_cast<int64_t>(value + PGDUCKDB_DUCK_TIMESTAMP_OFFSET)), offset);
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
			throw duckdb::InternalException("Unrecognized physical type (%s) for DECIMAL value",
			                                duckdb::EnumUtil::ToString(physical_type));
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
			((uint8_t *)&duckdb_uuid)[UUID_LEN - 1 - i] = ((uint8_t *)uuid)[i];
		}
		duckdb_uuid.upper ^= (uint64_t(1) << 63);
		Append(result, duckdb_uuid, offset);
		break;
	}
	case duckdb::LogicalTypeId::LIST: {
		// Convert Datum to ArrayType
		auto array = DatumGetArrayTypeP(value);

		auto ndims = ARR_NDIM(array);
		int *dims = ARR_DIMS(array);

		int16 typlen;
		bool typbyval;
		char typalign;
		get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);

		int nelems;
		Datum *elems;
		bool *nulls;
		// Deconstruct the array into Datum elements
		deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elems, &nulls, &nelems);

		if (ndims == -1) {
			throw duckdb::InternalException("Array type has an ndims of -1, so it's actually not an array??");
		}
		// Set the list_entry_t metadata
		duckdb::Vector *vec = &result;
		int write_offset = offset;
		for (int dim = 0; dim < ndims; dim++) {
			auto previous_dimension = dim ? dims[dim - 1] : 1;
			auto dimension = dims[dim];
			if (vec->GetType().id() != duckdb::LogicalTypeId::LIST) {
				throw duckdb::InvalidInputException(
				    "Dimensionality of the schema and the data does not match, data contains more dimensions than the "
				    "amount of dimensions specified by the schema");
			}
			auto child_offset = duckdb::ListVector::GetListSize(*vec);
			auto list_data = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*vec);
			for (int entry = 0; entry < previous_dimension; entry++) {
				list_data[write_offset + entry] = duckdb::list_entry_t(
				    // All lists in a postgres row are enforced to have the same dimension
				    // [[1,2],[2,3,4]] is not allowed, second list has 3 elements instead of 2
				    child_offset + (dimension * entry), dimension);
			}
			auto new_child_size = child_offset + (dimension * previous_dimension);
			duckdb::ListVector::Reserve(*vec, new_child_size);
			duckdb::ListVector::SetListSize(*vec, new_child_size);
			write_offset = child_offset;
			auto &child = duckdb::ListVector::GetEntry(*vec);
			vec = &child;
		}
		if (ndims == 0) {
			D_ASSERT(nelems == 0);
			auto child_offset = duckdb::ListVector::GetListSize(*vec);
			auto list_data = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*vec);
			list_data[write_offset] = duckdb::list_entry_t(child_offset, 0);
			vec = &duckdb::ListVector::GetEntry(*vec);
		}

		if (vec->GetType().id() == duckdb::LogicalTypeId::LIST) {
			throw duckdb::InvalidInputException(
			    "Dimensionality of the schema and the data does not match, data contains fewer dimensions than the "
			    "amount of dimensions specified by the schema");
		}

		for (int i = 0; i < nelems; i++) {
			idx_t dest_idx = write_offset + i;
			if (nulls[i]) {
				auto &array_mask = duckdb::FlatVector::Validity(*vec);
				array_mask.SetInvalid(dest_idx);
				continue;
			}
			ConvertPostgresToDuckValue(elems[i], *vec, dest_idx);
		}
		break;
	}
	default:
		throw duckdb::NotImplementedException("(DuckDB/ConvertPostgresToDuckValue) Unsupported pgduckdb type: %s",
		                                      result.GetType().ToString().c_str());
		break;
	}
}

typedef struct HeapTupleReadState {
	bool m_slow = 0;
	int m_last_tuple_att = 0;
	uint32 m_page_tuple_offset = 0;
} HeapTupleReadState;

static Datum
HeapTupleFetchNextColumnDatum(TupleDesc tupleDesc, HeapTuple tuple, HeapTupleReadState &heap_tuple_read_state,
                              int att_num, bool *is_null) {
	HeapTupleHeader tup = tuple->t_data;
	bool hasnulls = HeapTupleHasNulls(tuple);
	int attnum;
	char *tp;
	uint32 off;
	bits8 *bp = tup->t_bits;
	bool slow = false;
	Datum value = (Datum)0;

	attnum = heap_tuple_read_state.m_last_tuple_att;

	if (attnum == 0) {
		/* Start from the first attribute */
		off = 0;
		heap_tuple_read_state.m_slow = false;
	} else {
		/* Restore state from previous execution */
		off = heap_tuple_read_state.m_page_tuple_offset;
		slow = heap_tuple_read_state.m_slow;
	}

	tp = (char *)tup + tup->t_hoff;

	for (; attnum < att_num; attnum++) {
		Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);

		if (hasnulls && att_isnull(attnum, bp)) {
			value = (Datum)0;
			*is_null = true;
			slow = true; /* can't use attcacheoff anymore */
			continue;
		}

		*is_null = false;

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

	heap_tuple_read_state.m_last_tuple_att = att_num;
	heap_tuple_read_state.m_page_tuple_offset = off;

	if (slow) {
		heap_tuple_read_state.m_slow = true;
	} else {
		heap_tuple_read_state.m_slow = false;
	}

	return value;
}

void
InsertTupleIntoChunk(duckdb::DataChunk &output, duckdb::shared_ptr<PostgresScanGlobalState> scan_global_state,
                     duckdb::shared_ptr<PostgresScanLocalState> scan_local_state, HeapTupleData *tuple) {
	HeapTupleReadState heap_tuple_read_state = {};

	if (scan_global_state->m_count_tuples_only) {
		scan_local_state->m_output_vector_size++;
		return;
	}

	/* FIXME: all calls to duckdb_malloc/duckdb_free should be changed in future */
	Datum *values = (Datum *)duckdb_malloc(sizeof(Datum) * scan_global_state->m_columns.size());
	bool *nulls = (bool *)duckdb_malloc(sizeof(bool) * scan_global_state->m_columns.size());

	bool valid_tuple = true;

	for (auto const &[columnIdx, valueIdx] : scan_global_state->m_columns) {
		values[valueIdx] = HeapTupleFetchNextColumnDatum(scan_global_state->m_tuple_desc, tuple, heap_tuple_read_state,
		                                                 columnIdx + 1, &nulls[valueIdx]);
		if (scan_global_state->m_filters &&
		    (scan_global_state->m_filters->filters.find(valueIdx) != scan_global_state->m_filters->filters.end())) {
			auto &filter = scan_global_state->m_filters->filters[valueIdx];
			valid_tuple = ApplyValueFilter(*filter, values[valueIdx], nulls[valueIdx],
			                               scan_global_state->m_tuple_desc->attrs[columnIdx].atttypid);
		}

		if (!valid_tuple) {
			break;
		}
	}

	for (idx_t idx = 0; valid_tuple && idx < scan_global_state->m_projections.size(); idx++) {
		auto &result = output.data[idx];
		if (nulls[idx]) {
			auto &array_mask = duckdb::FlatVector::Validity(result);
			array_mask.SetInvalid(scan_local_state->m_output_vector_size);
		} else {
			idx_t projectionColumnIdx = scan_global_state->m_columns[scan_global_state->m_projections[idx]];
			if (scan_global_state->m_tuple_desc->attrs[scan_global_state->m_projections[idx]].attlen == -1) {
				bool should_free = false;
				values[projectionColumnIdx] =
				    DetoastPostgresDatum(reinterpret_cast<varlena *>(values[projectionColumnIdx]), &should_free);
				ConvertPostgresToDuckValue(values[projectionColumnIdx], result, scan_local_state->m_output_vector_size);
				if (should_free) {
					duckdb_free(reinterpret_cast<void *>(values[projectionColumnIdx]));
				}
			} else {
				ConvertPostgresToDuckValue(values[projectionColumnIdx], result, scan_local_state->m_output_vector_size);
			}
		}
	}

	if (valid_tuple) {
		scan_local_state->m_output_vector_size++;
	}

	output.SetCardinality(scan_local_state->m_output_vector_size);
	output.Verify();

	scan_global_state->m_total_row_count++;

	duckdb_free(values);
	duckdb_free(nulls);
}

} // namespace pgduckdb
