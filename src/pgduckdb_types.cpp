#include "duckdb.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/blob.hpp"

#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"

extern "C" {

#include "pgduckdb/vendor/pg_numeric_c.hpp"

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/tupdesc_details.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/uuid.h"
#include "utils/array.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/jsonb.h"
}

#include "pgduckdb/pgduckdb_detoast.hpp"

namespace pgduckdb {

NumericVar FromNumeric(Numeric num);

struct NumericAsDouble : public duckdb::ExtraTypeInfo {
	// Dummy struct to indicate at conversion that the source is a Numeric
public:
	NumericAsDouble() : ExtraTypeInfo(duckdb::ExtraTypeInfoType::INVALID_TYPE_INFO) {
	}
};

// FIXME: perhaps we want to just make a generic ExtraTypeInfo that holds the Postgres type OID
struct IsBpChar : public duckdb::ExtraTypeInfo {
public:
	IsBpChar() : ExtraTypeInfo(duckdb::ExtraTypeInfoType::INVALID_TYPE_INFO) {
	}
};

using duckdb::hugeint_t;
using duckdb::uhugeint_t;

struct DecimalConversionInteger {
	static int64_t
	GetPowerOfTen(idx_t index) {
		static const int64_t POWERS_OF_TEN[] {1,
		                                      10,
		                                      100,
		                                      1000,
		                                      10000,
		                                      100000,
		                                      1000000,
		                                      10000000,
		                                      100000000,
		                                      1000000000,
		                                      10000000000,
		                                      100000000000,
		                                      1000000000000,
		                                      10000000000000,
		                                      100000000000000,
		                                      1000000000000000,
		                                      10000000000000000,
		                                      100000000000000000,
		                                      1000000000000000000};
		if (index >= 19) {
			throw duckdb::InternalException("DecimalConversionInteger::GetPowerOfTen - Out of range");
		}
		return POWERS_OF_TEN[index];
	}

	template <class T>
	static T
	Finalize(const NumericVar &, T result) {
		return result;
	}
};

struct DecimalConversionHugeint {
	static hugeint_t
	GetPowerOfTen(idx_t index) {
		static const hugeint_t POWERS_OF_TEN[] {
		    hugeint_t(1),
		    hugeint_t(10),
		    hugeint_t(100),
		    hugeint_t(1000),
		    hugeint_t(10000),
		    hugeint_t(100000),
		    hugeint_t(1000000),
		    hugeint_t(10000000),
		    hugeint_t(100000000),
		    hugeint_t(1000000000),
		    hugeint_t(10000000000),
		    hugeint_t(100000000000),
		    hugeint_t(1000000000000),
		    hugeint_t(10000000000000),
		    hugeint_t(100000000000000),
		    hugeint_t(1000000000000000),
		    hugeint_t(10000000000000000),
		    hugeint_t(100000000000000000),
		    hugeint_t(1000000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10),
		    hugeint_t(1000000000000000000) * hugeint_t(100),
		    hugeint_t(1000000000000000000) * hugeint_t(1000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(10),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(100)};
		if (index >= 39) {
			throw duckdb::InternalException("DecimalConversionHugeint::GetPowerOfTen - Out of range");
		}
		return POWERS_OF_TEN[index];
	}

	static hugeint_t
	Finalize(const NumericVar &, hugeint_t result) {
		return result;
	}
};

struct DecimalConversionDouble {
	static double
	GetPowerOfTen(idx_t index) {
		return pow(10, double(index));
	}

	static double
	Finalize(const NumericVar &numeric, double result) {
		return result / GetPowerOfTen(numeric.dscale);
	}
};

static inline Datum
ConvertBoolDatum(const duckdb::Value &value) {
	return value.GetValue<bool>();
}

static inline Datum
ConvertCharDatum(const duckdb::Value &value) {
	return value.GetValue<int8_t>();
}

static inline Datum
ConvertInt2Datum(const duckdb::Value &value) {
	if (value.type().id() == duckdb::LogicalTypeId::UTINYINT) {
		return UInt8GetDatum(value.GetValue<uint8_t>());
	}
	return Int16GetDatum(value.GetValue<int16_t>());
}

static inline Datum
ConvertInt4Datum(const duckdb::Value &value) {
	if (value.type().id() == duckdb::LogicalTypeId::USMALLINT) {
		return UInt16GetDatum(value.GetValue<uint16_t>());
	}
	return Int32GetDatum(value.GetValue<int32_t>());
}

static inline Datum
ConvertInt8Datum(const duckdb::Value &value) {
	if (value.type().id() == duckdb::LogicalTypeId::UINTEGER) {
		return UInt32GetDatum(value.GetValue<uint32_t>());
	}
	return Int64GetDatum(value.GetValue<int64_t>());
}

static Datum
ConvertVarCharDatum(const duckdb::Value &value) {
	auto str = value.GetValue<duckdb::string>();
	auto varchar = str.c_str();
	auto varchar_len = str.size();

	text *result = (text *)palloc0(varchar_len + VARHDRSZ);
	SET_VARSIZE(result, varchar_len + VARHDRSZ);
	memcpy(VARDATA(result), varchar, varchar_len);
	return PointerGetDatum(result);
}

static Datum
ConvertBinaryDatum(const duckdb::Value &value) {
	auto str = value.GetValueUnsafe<duckdb::string_t>();
	auto blob_len = str.GetSize();
	auto blob = str.GetDataUnsafe();
	bytea *result = (bytea *)palloc0(blob_len + VARHDRSZ);
	SET_VARSIZE(result, blob_len + VARHDRSZ);
	memcpy(VARDATA(result), blob, blob_len);
	return PointerGetDatum(result);
}

inline Datum
ConvertDateDatum(const duckdb::Value &value) {
	duckdb::date_t date = value.GetValue<duckdb::date_t>();
	return date.days - pgduckdb::PGDUCKDB_DUCK_DATE_OFFSET;
}

static Datum
ConvertIntervalDatum(const duckdb::Value &value) {
	duckdb::interval_t duckdb_interval = value.GetValue<duckdb::interval_t>();
	Interval *pg_interval = static_cast<Interval *>(palloc(sizeof(Interval)));
	pg_interval->month = duckdb_interval.months;
	pg_interval->day = duckdb_interval.days;
	pg_interval->time = duckdb_interval.micros;
	return IntervalPGetDatum(pg_interval);
}

inline Datum
ConvertTimestampDatum(const duckdb::Value &value) {
	// Extract raw int64_t value of timestamp
	int64_t rawValue = value.GetValue<int64_t>();

	// Handle specific Timestamp unit(sec, ms, ns) types
	switch (value.type().id()) {
	case duckdb::LogicalType::TIMESTAMP_MS:
		// 1 ms = 10^3 micro-sec
		rawValue *= 1000;
		break;
	case duckdb::LogicalType::TIMESTAMP_NS:
		// 1 ns = 10^-3 micro-sec
		rawValue /= 1000;
		break;
	case duckdb::LogicalType::TIMESTAMP_S:
		// 1 s = 10^6 micro-sec
		rawValue *= 1000000;
		break;
	default:
		// Since we don't want to handle anything here
		break;
	}
	return rawValue - pgduckdb::PGDUCKDB_DUCK_TIMESTAMP_OFFSET;
}

inline Datum
ConvertFloatDatum(const duckdb::Value &value) {
	return Float4GetDatum(value.GetValue<float>());
}

inline Datum
ConvertDoubleDatum(const duckdb::Value &value) {
	return Float8GetDatum(value.GetValue<double>());
}

template <class T, class OP = DecimalConversionInteger>
void
ConvertNumeric(const duckdb::Value &ddb_value, idx_t scale, NumericVar &result) {
	result.dscale = scale;

	T value = ddb_value.GetValueUnsafe<T>();
	if (value < 0) {
		value = -value;
		result.sign = NUMERIC_NEG;
	} else {
		result.sign = NUMERIC_POS;
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

	constexpr idx_t MAX_DIGITS = sizeof(T) * 4;
	uint16_t integral_digits[MAX_DIGITS];
	uint16_t fractional_digits[MAX_DIGITS];
	int32_t integral_ndigits;

	// split the integral part into parts of up to NBASE (4 digits => 0..9999)
	integral_ndigits = 0;
	while (integer_part > 0) {
		integral_digits[integral_ndigits++] = uint16_t(integer_part % T(NBASE));
		integer_part /= T(NBASE);
	}

	result.weight = integral_ndigits - 1;
	// split the fractional part into parts of up to NBASE (4 digits => 0..9999)
	// count the amount of digits required for the fractional part
	// note that while it is technically possible to leave out zeros here this adds even more complications
	// so we just always write digits for the full "scale", even if not strictly required
	idx_t fractional_ndigits = (scale + DEC_DIGITS - 1) / DEC_DIGITS;
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

	result.ndigits = integral_ndigits + fractional_ndigits;

	result.buf = (NumericDigit *)palloc(result.ndigits * sizeof(NumericDigit));
	result.digits = result.buf;
	auto &digits = result.digits;

	idx_t digits_idx = 0;
	for (idx_t i = integral_ndigits; i > 0; i--) {
		digits[digits_idx++] = integral_digits[i - 1];
	}
	for (idx_t i = fractional_ndigits; i > 0; i--) {
		digits[digits_idx++] = fractional_digits[i - 1];
	}
}

static Datum
ConvertNumericDatum(const duckdb::Value &value) {
	auto value_type_id = value.type().id();
	if (value_type_id == duckdb::LogicalTypeId::DOUBLE) {
		return ConvertDoubleDatum(value);
	}

	NumericVar numeric_var;
	D_ASSERT(value_type_id == duckdb::LogicalTypeId::DECIMAL || value_type_id == duckdb::LogicalTypeId::HUGEINT ||
	         value_type_id == duckdb::LogicalTypeId::UBIGINT || value_type_id == duckdb::LogicalTypeId::UHUGEINT);
	const bool is_decimal = value_type_id == duckdb::LogicalTypeId::DECIMAL;
	uint8_t scale = is_decimal ? duckdb::DecimalType::GetScale(value.type()) : 0;

	switch (value.type().InternalType()) {
	case duckdb::PhysicalType::INT16:
		ConvertNumeric<int16_t>(value, scale, numeric_var);
		break;
	case duckdb::PhysicalType::INT32:
		ConvertNumeric<int32_t>(value, scale, numeric_var);
		break;
	case duckdb::PhysicalType::INT64:
		ConvertNumeric<int64_t>(value, scale, numeric_var);
		break;
	case duckdb::PhysicalType::UINT64:
		ConvertNumeric<uint64_t>(value, scale, numeric_var);
		break;
	case duckdb::PhysicalType::INT128:
		ConvertNumeric<hugeint_t, DecimalConversionHugeint>(value, scale, numeric_var);
		break;
	case duckdb::PhysicalType::UINT128:
		ConvertNumeric<uhugeint_t, DecimalConversionHugeint>(value, scale, numeric_var);
		break;
	default:
		throw duckdb::InvalidInputException(
		    "(PGDuckDB/ConvertNumericDatum) Unrecognized physical type for DECIMAL value");
	}

	auto numeric = PostgresFunctionGuard(make_result, &numeric_var);
	return NumericGetDatum(numeric);
}

static Datum
ConvertUUIDDatum(const duckdb::Value &value) {
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

	return UUIDPGetDatum(postgres_uuid);
}

static duckdb::interval_t
DatumGetInterval(Datum value) {
	Interval *pg_interval = DatumGetIntervalP(value);
	duckdb::interval_t duck_interval;
	duck_interval.months = pg_interval->month;
	duck_interval.days = pg_interval->day;
	duck_interval.micros = pg_interval->time;
	return duck_interval;
}

template <int32_t OID>
struct PostgresTypeTraits;

// Specializations for each type
// BOOL type
template <>
struct PostgresTypeTraits<BOOLOID> {
	static constexpr int16_t typlen = 1;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'c';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertBoolDatum(val);
	}
};

// CHAR type
template <>
struct PostgresTypeTraits<CHAROID> {
	static constexpr int16_t typlen = 1;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'c';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertCharDatum(val);
	}
};

// INT2 type (smallint)
template <>
struct PostgresTypeTraits<INT2OID> {
	static constexpr int16_t typlen = 2;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 's';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertInt2Datum(val);
	}
};

// INT4 type (integer)
template <>
struct PostgresTypeTraits<INT4OID> {
	static constexpr int16_t typlen = 4;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertInt4Datum(val);
	}
};

// INT8 type (bigint)
template <>
struct PostgresTypeTraits<INT8OID> {
	static constexpr int16_t typlen = 8;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'd';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertInt8Datum(val);
	}
};

// FLOAT4 type (real)
template <>
struct PostgresTypeTraits<FLOAT4OID> {
	static constexpr int16_t typlen = 4;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertFloatDatum(val);
	}
};

// FLOAT8 type (double precision)
template <>
struct PostgresTypeTraits<FLOAT8OID> {
	static constexpr int16_t typlen = 8;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'd';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertDoubleDatum(val);
	}
};

// TIMESTAMP type
template <>
struct PostgresTypeTraits<TIMESTAMPOID> {
	static constexpr int16_t typlen = 8;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'd';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertTimestampDatum(val);
	}
};

// INTERVAL type
template <>
struct PostgresTypeTraits<INTERVALOID> {
	static constexpr int16_t typlen = 16;
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'c';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertIntervalDatum(val);
	}
};

// DATE type
template <>
struct PostgresTypeTraits<DATEOID> {
	static constexpr int16_t typlen = 4;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertDateDatum(val);
	}
};

// UUID type
template <>
struct PostgresTypeTraits<UUIDOID> {
	static constexpr int16_t typlen = 16;
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'c';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertUUIDDatum(val);
	}
};

// NUMERIC type
template <>
struct PostgresTypeTraits<NUMERICOID> {
	static constexpr int16_t typlen = -1; // variable-length
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertNumericDatum(val);
	}
};

// VARCHAR type
template <>
struct PostgresTypeTraits<VARCHAROID> {
	static constexpr int16_t typlen = -1; // variable-length
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertVarCharDatum(val);
	}
};

// BLOB type
template <>
struct PostgresTypeTraits<BYTEAOID> {
	static constexpr int16_t typlen = -1; // variable-length
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertBinaryDatum(val);
	}
};

template <int32_t OID>
struct PostgresOIDMapping {
	static constexpr int32_t postgres_oid = OID;
	static constexpr int16_t typlen = PostgresTypeTraits<OID>::typlen;
	static constexpr bool typbyval = PostgresTypeTraits<OID>::typbyval;
	static constexpr char typalign = PostgresTypeTraits<OID>::typalign;

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return PostgresTypeTraits<OID>::ToDatum(val);
	}
};

template <class MAPPING>
struct PODArray {
public:
	static ArrayType *
	ConstructArray(Datum *datums, bool *nulls, int ndims, int *dims, int *lower_bound) {
		return construct_md_array(datums, nulls, ndims, dims, lower_bound, MAPPING::postgres_oid, MAPPING::typlen,
		                          MAPPING::typbyval, MAPPING::typalign);
	}

	static Datum
	ConvertToPostgres(const duckdb::Value &val) {
		return MAPPING::ToDatum(val);
	}
};

using BoolArray = PODArray<PostgresOIDMapping<BOOLOID>>;
using CharArray = PODArray<PostgresOIDMapping<CHAROID>>;
using Int2Array = PODArray<PostgresOIDMapping<INT2OID>>;
using Int4Array = PODArray<PostgresOIDMapping<INT4OID>>;
using Int8Array = PODArray<PostgresOIDMapping<INT8OID>>;
using Float4Array = PODArray<PostgresOIDMapping<FLOAT4OID>>;
using Float8Array = PODArray<PostgresOIDMapping<FLOAT8OID>>;
using DateArray = PODArray<PostgresOIDMapping<DATEOID>>;
using TimestampArray = PODArray<PostgresOIDMapping<TIMESTAMPOID>>;
using IntervalArray = PODArray<PostgresOIDMapping<INTERVALOID>>;
using UUIDArray = PODArray<PostgresOIDMapping<UUIDOID>>;
using VarCharArray = PODArray<PostgresOIDMapping<VARCHAROID>>;
using NumericArray = PODArray<PostgresOIDMapping<NUMERICOID>>;
using ByteArray = PODArray<PostgresOIDMapping<BYTEAOID>>;

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
	PostgresArrayAppendState(idx_t _number_of_dimensions) : number_of_dimensions(_number_of_dimensions) {
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
		int to_append = values.size();

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
			for (auto &child_val : values) {
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

			for (auto &child_val : values) {
				nulls[count] = child_val.IsNull();
				if (!nulls[count]) {
					datums[count] = OP::ConvertToPostgres(child_val);
				}
				++count;
			}
		}
	}

private:
	idx_t count = 0;

public:
	idx_t expected_values = 1;
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
	auto number_of_dimensions = GetDuckDBListDimensionality(value.type());

	PostgresArrayAppendState<OP> append_state(number_of_dimensions);
	append_state.AppendValueAtDimension(value, 0);

	// Create the array
	auto datums = append_state.datums;
	auto nulls = append_state.nulls;
	auto dimensions = append_state.dimensions;
	auto lower_bounds = append_state.lower_bounds;

	// When we insert an empty array into multi-dimensions array,
	// the dimensions[1] to dimension[number_of_dimensions-1] will not be set and always be -1.
	for (idx_t i = 0; i < number_of_dimensions; i++) {
		if (dimensions[i] == -1) {
			// This dimension is not set yet, we should set them to 0.
			// Otherwise, it will cause some issues when we call ConstructArray.
			dimensions[i] = 0;
		}
	}

	auto arr = OP::ConstructArray(datums, nulls, number_of_dimensions, dimensions, lower_bounds);

	// Free allocated memory
	if (append_state.expected_values > 0) {
		pfree(datums);
		pfree(nulls);
	}
	pfree(dimensions);
	pfree(lower_bounds);

	slot->tts_values[col] = PointerGetDatum(arr);
}

bool
ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, idx_t col) {
	Oid oid = slot->tts_tupleDescriptor->attrs[col].atttypid;

	switch (oid) {
	case BOOLOID:
		slot->tts_values[col] = ConvertBoolDatum(value);
		break;
	case CHAROID:
		slot->tts_values[col] = ConvertCharDatum(value);
		break;
	case INT2OID: {
		slot->tts_values[col] = ConvertInt2Datum(value);
		break;
	}
	case INT4OID: {
		slot->tts_values[col] = ConvertInt4Datum(value);
		break;
	}
	case INT8OID: {
		slot->tts_values[col] = ConvertInt8Datum(value);
		break;
	}
	case BPCHAROID:
	case TEXTOID:
	case JSONOID:
	case VARCHAROID: {
		slot->tts_values[col] = ConvertVarCharDatum(value);
		break;
	}
	case DATEOID: {
		slot->tts_values[col] = ConvertDateDatum(value);
		break;
	}
	case TIMESTAMPOID: {
		slot->tts_values[col] = ConvertTimestampDatum(value);
		break;
	}
	case TIMESTAMPTZOID: {
		duckdb::timestamp_t timestamp = value.GetValue<duckdb::timestamp_t>();
		slot->tts_values[col] = timestamp.value - pgduckdb::PGDUCKDB_DUCK_TIMESTAMP_OFFSET;
		break;
	}
	case INTERVALOID: {
		slot->tts_values[col] = ConvertIntervalDatum(value);
		break;
	}
	case FLOAT4OID: {
		slot->tts_values[col] = ConvertFloatDatum(value);
		break;
	}
	case FLOAT8OID: {
		slot->tts_values[col] = ConvertDoubleDatum(value);
		break;
	}
	case NUMERICOID: {
		slot->tts_values[col] = ConvertNumericDatum(value);
		break;
	}
	case UUIDOID: {
		slot->tts_values[col] = ConvertUUIDDatum(value);
		break;
	}
	case BYTEAOID: {
		slot->tts_values[col] = ConvertBinaryDatum(value);
		break;
	}
	case BOOLARRAYOID: {
		ConvertDuckToPostgresArray<BoolArray>(slot, value, col);
		break;
	}
	case CHARARRAYOID: {
		ConvertDuckToPostgresArray<CharArray>(slot, value, col);
		break;
	}
	case INT2ARRAYOID: {
		ConvertDuckToPostgresArray<Int2Array>(slot, value, col);
		break;
	}
	case INT4ARRAYOID: {
		ConvertDuckToPostgresArray<Int4Array>(slot, value, col);
		break;
	}
	case INT8ARRAYOID: {
		ConvertDuckToPostgresArray<Int8Array>(slot, value, col);
		break;
	}
	case BPCHARARRAYOID:
	case TEXTARRAYOID:
	case JSONARRAYOID:
	case VARCHARARRAYOID: {
		ConvertDuckToPostgresArray<VarCharArray>(slot, value, col);
		break;
	}
	case DATEARRAYOID: {
		ConvertDuckToPostgresArray<DateArray>(slot, value, col);
		break;
	}
	case TIMESTAMPARRAYOID: {
		ConvertDuckToPostgresArray<TimestampArray>(slot, value, col);
		break;
	}
	case INTERVALARRAYOID: {
		ConvertDuckToPostgresArray<IntervalArray>(slot, value, col);
		break;
	}
	case FLOAT4ARRAYOID: {
		ConvertDuckToPostgresArray<Float4Array>(slot, value, col);
		break;
	}
	case FLOAT8ARRAYOID: {
		ConvertDuckToPostgresArray<Float8Array>(slot, value, col);
		break;
	}
	case NUMERICARRAYOID: {
		ConvertDuckToPostgresArray<NumericArray>(slot, value, col);
		break;
	}
	case UUIDARRAYOID: {
		ConvertDuckToPostgresArray<UUIDArray>(slot, value, col);
		break;
	}
	case BYTEAARRAYOID: {
		ConvertDuckToPostgresArray<ByteArray>(slot, value, col);
		break;
	}
	default:
		elog(WARNING, "(PGDuckDB/ConvertDuckToPostgresValue) Unsuported pgduckdb type: %d", oid);
		return false;
	}
	return true;
}

static inline int32
make_numeric_typmod(int precision, int scale) {
	return ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ;
}

static inline int
numeric_typmod_precision(int32 typmod) {
	return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

static inline int
numeric_typmod_scale(int32 typmod) {
	return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}

duckdb::LogicalType
ConvertPostgresToBaseDuckColumnType(Form_pg_attribute &attribute) {
	switch (attribute->atttypid) {
	case BOOLOID:
	case BOOLARRAYOID:
		return duckdb::LogicalTypeId::BOOLEAN;
	case CHAROID:
	case CHARARRAYOID:
		return duckdb::LogicalTypeId::TINYINT;
	case INT2OID:
	case INT2ARRAYOID:
		return duckdb::LogicalTypeId::SMALLINT;
	case INT4OID:
	case INT4ARRAYOID:
		return duckdb::LogicalTypeId::INTEGER;
	case INT8OID:
	case INT8ARRAYOID:
		return duckdb::LogicalTypeId::BIGINT;
	case BPCHAROID:
	case BPCHARARRAYOID:
	case TEXTOID:
	case TEXTARRAYOID:
	case VARCHAROID:
	case VARCHARARRAYOID:
		return duckdb::LogicalTypeId::VARCHAR;
	case DATEOID:
	case DATEARRAYOID:
		return duckdb::LogicalTypeId::DATE;
	case TIMESTAMPOID:
	case TIMESTAMPARRAYOID:
		return duckdb::LogicalTypeId::TIMESTAMP;
	case TIMESTAMPTZOID:
		return duckdb::LogicalTypeId::TIMESTAMP_TZ;
	case INTERVALOID:
	case INTERVALARRAYOID:
		return duckdb::LogicalTypeId::INTERVAL;
	case FLOAT4OID:
	case FLOAT4ARRAYOID:
		return duckdb::LogicalTypeId::FLOAT;
	case FLOAT8OID:
	case FLOAT8ARRAYOID:
		return duckdb::LogicalTypeId::DOUBLE;
	case NUMERICOID:
	case NUMERICARRAYOID: {
		auto &typmod = attribute->atttypmod;
		auto precision = numeric_typmod_precision(typmod);
		auto scale = numeric_typmod_scale(typmod);
		if (typmod == -1 || precision < 0 || scale < 0 || precision > 38) {
			auto extra_type_info = duckdb::make_shared_ptr<NumericAsDouble>();
			return duckdb::LogicalType(duckdb::LogicalTypeId::DOUBLE, std::move(extra_type_info));
		}

		return duckdb::LogicalType::DECIMAL(precision, scale);
	}
	case UUIDOID:
	case UUIDARRAYOID:
		return duckdb::LogicalTypeId::UUID;
	case JSONOID:
	case JSONARRAYOID:
	case JSONBOID:
	case JSONBARRAYOID:
		return duckdb::LogicalType::JSON();
	case REGCLASSOID:
	case REGCLASSARRAYOID:
		return duckdb::LogicalTypeId::UINTEGER;
	case BYTEAOID:
	case BYTEAARRAYOID:
		return duckdb::LogicalTypeId::BLOB;
	default:
		return duckdb::LogicalType::USER("UnsupportedPostgresType (Oid=" + std::to_string(attribute->atttypid) + ")");
	}
}

duckdb::LogicalType
ConvertPostgresToDuckColumnType(Form_pg_attribute &attribute) {
	auto base_type = ConvertPostgresToBaseDuckColumnType(attribute);
	auto dimensions = attribute->attndims;
	if (dimensions == 0) {
		return base_type;
	}

	for (int i = 0; i < dimensions; i++) {
		base_type = duckdb::LogicalType::LIST(base_type);
	}
	return base_type;
}

Oid
GetPostgresArrayDuckDBType(const duckdb::LogicalType &type) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		return BOOLARRAYOID;
	case duckdb::LogicalTypeId::TINYINT:
		return CHARARRAYOID;
	case duckdb::LogicalTypeId::SMALLINT:
		return INT2ARRAYOID;
	case duckdb::LogicalTypeId::INTEGER:
		return INT4ARRAYOID;
	case duckdb::LogicalTypeId::BIGINT:
		return INT8ARRAYOID;
	case duckdb::LogicalTypeId::HUGEINT:
		return NUMERICARRAYOID;
	case duckdb::LogicalTypeId::UTINYINT:
		return INT2ARRAYOID;
	case duckdb::LogicalTypeId::USMALLINT:
		return INT4ARRAYOID;
	case duckdb::LogicalTypeId::UINTEGER:
		return INT8ARRAYOID;
	case duckdb::LogicalTypeId::VARCHAR:
		return type.IsJSONType() ? JSONARRAYOID : VARCHARARRAYOID;
	case duckdb::LogicalTypeId::DATE:
		return DATEARRAYOID;
	case duckdb::LogicalTypeId::TIMESTAMP:
		return TIMESTAMPARRAYOID;
	case duckdb::LogicalTypeId::INTERVAL:
		return INTERVALARRAYOID;
	case duckdb::LogicalTypeId::FLOAT:
		return FLOAT4ARRAYOID;
	case duckdb::LogicalTypeId::DOUBLE:
		return FLOAT8ARRAYOID;
	case duckdb::LogicalTypeId::DECIMAL:
		return NUMERICARRAYOID;
	case duckdb::LogicalTypeId::UUID:
		return UUIDARRAYOID;
	case duckdb::LogicalTypeId::BLOB:
		return BYTEAARRAYOID;
	default: {
		elog(WARNING, "(PGDuckDB/GetPostgresDuckDBType) Unsupported `LIST` subtype %d to Postgres type",
		     static_cast<uint8_t>(type.id()));
		return InvalidOid;
	}
	}
}

Oid
GetPostgresDuckDBType(const duckdb::LogicalType &type) {
	switch (type.id()) {
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
	case duckdb::LogicalTypeId::UBIGINT:
	case duckdb::LogicalTypeId::HUGEINT:
	case duckdb::LogicalTypeId::UHUGEINT:
		return NUMERICOID;
	case duckdb::LogicalTypeId::UTINYINT:
		return INT2OID;
	case duckdb::LogicalTypeId::USMALLINT:
		return INT4OID;
	case duckdb::LogicalTypeId::UINTEGER:
		return INT8OID;
	case duckdb::LogicalTypeId::VARCHAR:
		return type.IsJSONType() ? JSONOID : VARCHAROID;
	case duckdb::LogicalTypeId::DATE:
		return DATEOID;
	case duckdb::LogicalTypeId::TIMESTAMP:
	case duckdb::LogicalTypeId::TIMESTAMP_SEC:
	case duckdb::LogicalTypeId::TIMESTAMP_MS:
	case duckdb::LogicalTypeId::TIMESTAMP_NS:
		return TIMESTAMPOID;
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		return TIMESTAMPTZOID;
	case duckdb::LogicalTypeId::INTERVAL:
		return INTERVALOID;
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
		return GetPostgresArrayDuckDBType(*duck_type);
	}
	case duckdb::LogicalTypeId::BLOB:
		return BYTEAOID;
	default: {
		elog(WARNING, "(PGDuckDB/GetPostgresDuckDBType) Could not convert DuckDB type: %s to Postgres type",
		     type.ToString().c_str());
		return InvalidOid;
	}
	}
}

int32
GetPostgresDuckDBTypemod(const duckdb::LogicalType &type) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::DECIMAL: {
		uint8_t width, scale;
		type.GetDecimalProperties(width, scale);
		return make_numeric_typmod(width, scale);
	}
	default:
		return -1;
	}
}

template <class T>
static void
Append(duckdb::Vector &result, T value, idx_t offset) {
	auto data = duckdb::FlatVector::GetData<T>(result);
	data[offset] = value;
}

static void
AppendString(duckdb::Vector &result, Datum value, idx_t offset, bool is_bpchar) {
	const char *text = VARDATA_ANY(value);
	/* Remove the padding of a BPCHAR type. DuckDB expects unpadded value. */
	auto len = is_bpchar ? bpchartruelen(VARDATA_ANY(value), VARSIZE_ANY_EXHDR(value)) : VARSIZE_ANY_EXHDR(value);
	duckdb::string_t str(text, len);

	auto data = duckdb::FlatVector::GetData<duckdb::string_t>(result);
	data[offset] = duckdb::StringVector::AddString(result, str);
}

static void
AppendJsonb(duckdb::Vector &result, Datum value, idx_t offset) {
	auto jsonb = DatumGetJsonbP(value);
	auto jsonb_str = JsonbToCString(NULL, &jsonb->root, VARSIZE(jsonb));
	duckdb::string_t str(jsonb_str);
	auto data = duckdb::FlatVector::GetData<duckdb::string_t>(result);
	data[offset] = duckdb::StringVector::AddString(result, str);
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
		int32_t digit_index = 0;
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
	return numeric.sign == NUMERIC_NEG ? -base_res : base_res;
}

/*
 * Convert a Postgres Datum to a DuckDB Value. This is meant to be used to
 * covert query parameters in a prepared statement to its DuckDB equivalent.
 * Passing it a Datum that is stored on disk results in undefined behavior,
 * because this fuction makes no effert to detoast the Datum.
 */
duckdb::Value
ConvertPostgresParameterToDuckValue(Datum value, Oid postgres_type) {
	switch (postgres_type) {
	case BOOLOID:
		return duckdb::Value::BOOLEAN(DatumGetBool(value));
	case INT2OID:
		return duckdb::Value::SMALLINT(DatumGetInt16(value));
	case INT4OID:
		return duckdb::Value::INTEGER(DatumGetInt32(value));
	case INT8OID:
		return duckdb::Value::BIGINT(DatumGetInt64(value));
	case BPCHAROID:
	case TEXTOID:
	case JSONOID:
	case VARCHAROID: {
		// FIXME: TextDatumGetCstring allocates so it needs a
		// guard, but it's a macro not a function, so our current gaurd
		// template does not handle it.
		return duckdb::Value(TextDatumGetCString(value));
	}
	case DATEOID:
		return duckdb::Value::DATE(duckdb::date_t(DatumGetDateADT(value) + PGDUCKDB_DUCK_DATE_OFFSET));
	case TIMESTAMPOID:
		return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(DatumGetTimestamp(value) + PGDUCKDB_DUCK_TIMESTAMP_OFFSET));
	case TIMESTAMPTZOID:
		return duckdb::Value::TIMESTAMPTZ(
		    duckdb::timestamp_t(DatumGetTimestampTz(value) + PGDUCKDB_DUCK_TIMESTAMP_OFFSET));
	case INTERVALOID:
		return duckdb::Value::INTERVAL(DatumGetInterval(value));
	case FLOAT4OID:
		return duckdb::Value::FLOAT(DatumGetFloat4(value));
	case FLOAT8OID:
		return duckdb::Value::DOUBLE(DatumGetFloat8(value));
	default:
		elog(ERROR, "Could not convert Postgres parameter of type: %d to DuckDB type", postgres_type);
	}
}

void
ConvertPostgresToDuckValue(Oid attr_type, Datum value, duckdb::Vector &result, idx_t offset) {
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
		if (attr_type == JSONBOID) {
			AppendJsonb(result, value, offset);
			break;
		} else {
			AppendString(result, value, offset, attr_type == BPCHAROID);
		}
		break;
	}
	case duckdb::LogicalTypeId::DATE:
		Append<duckdb::date_t>(result, duckdb::date_t(static_cast<int32_t>(value + PGDUCKDB_DUCK_DATE_OFFSET)), offset);
		break;

	case duckdb::LogicalTypeId::TIMESTAMP_SEC:
	case duckdb::LogicalTypeId::TIMESTAMP_MS:
	case duckdb::LogicalTypeId::TIMESTAMP_NS:
	case duckdb::LogicalTypeId::TIMESTAMP:
		Append<duckdb::timestamp_t>(
		    result, duckdb::timestamp_t(static_cast<int64_t>(value + PGDUCKDB_DUCK_TIMESTAMP_OFFSET)), offset);
		break;
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		Append<duckdb::timestamp_t>(
		    result, duckdb::timestamp_t(static_cast<int64_t>(value + PGDUCKDB_DUCK_TIMESTAMP_OFFSET)), offset);
		break;
	case duckdb::LogicalTypeId::INTERVAL:
		Append<duckdb::interval_t>(result, DatumGetInterval(value), offset);
		break;
	case duckdb::LogicalTypeId::FLOAT:
		Append<float>(result, DatumGetFloat4(value), offset);
		break;
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
		default:
			throw duckdb::InternalException("Unrecognized physical type (%s) for DECIMAL value",
			                                duckdb::EnumUtil::ToString(physical_type));
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
	case duckdb::LogicalTypeId::BLOB: {
		const char *bytea_data = VARDATA_ANY(value);
		size_t bytea_length = VARSIZE_ANY_EXHDR(value);
		const duckdb::string_t s(bytea_data, bytea_length);
		auto data = duckdb::FlatVector::GetData<duckdb::string_t>(result);
		data[offset] = duckdb::StringVector::AddStringOrBlob(result, s);
		break;
	}
	case duckdb::LogicalTypeId::LIST: {
		// Convert Datum to ArrayType
		auto array = DatumGetArrayTypeP(value);

		auto ndims = ARR_NDIM(array);
		int *dims = ARR_DIMS(array);
		auto elem_type = ARR_ELEMTYPE(array);

		int16 typlen;
		bool typbyval;
		char typalign;
		get_typlenbyvalalign(elem_type, &typlen, &typbyval, &typalign);

		int nelems;
		Datum *elems;
		bool *nulls;
		// Deconstruct the array into Datum elements
		deconstruct_array(array, elem_type, typlen, typbyval, typalign, &elems, &nulls, &nelems);

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
		} else if (vec->GetType().id() == duckdb::LogicalTypeId::LIST) {
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
			ConvertPostgresToDuckValue(elem_type, elems[i], *vec, dest_idx);
		}
		break;
	}
	default:
		throw duckdb::NotImplementedException("(DuckDB/ConvertPostgresToDuckValue) Unsupported pgduckdb type: %s",
		                                      result.GetType().ToString().c_str());
	}
}

void
InsertTupleIntoChunk(duckdb::DataChunk &output, PostgresScanLocalState &scan_local_state, TupleTableSlot *slot) {

	auto scan_global_state = scan_local_state.global_state;

	if (scan_global_state->count_tuples_only) {
		/* COUNT(*) returned tuple will have only one value returned as first tuple element. */
		scan_global_state->total_row_count += slot->tts_values[0];
		scan_local_state.output_vector_size += slot->tts_values[0];
		return;
	}
	/* Write tuple columns in output vector. */
	for (int duckdb_output_index = 0; duckdb_output_index < slot->tts_tupleDescriptor->natts; duckdb_output_index++) {
		auto &result = output.data[duckdb_output_index];
		if (slot->tts_isnull[duckdb_output_index]) {
			auto &array_mask = duckdb::FlatVector::Validity(result);
			array_mask.SetInvalid(scan_local_state.output_vector_size);
		} else {
			/* Use returned tuple slot attr information. */
			auto attr = slot->tts_tupleDescriptor->attrs[duckdb_output_index];
			ConvertPostgresToDuckValue(attr.atttypid, slot->tts_values[duckdb_output_index], result,
			                           scan_local_state.output_vector_size);
		}
	}

	scan_local_state.output_vector_size++;
	scan_global_state->total_row_count++;
}

NumericVar
FromNumeric(Numeric num) {
	NumericVar dest;
	dest.ndigits = NUMERIC_NDIGITS(num);
	dest.weight = NUMERIC_WEIGHT(num);
	dest.sign = NUMERIC_SIGN(num);
	dest.dscale = NUMERIC_DSCALE(num);
	dest.digits = NUMERIC_DIGITS(num);
	dest.buf = NULL; /* digits array is not palloc'd */
	return dest;
}

} // namespace pgduckdb
