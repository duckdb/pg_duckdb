#include "pgduckdb/pgduckdb_filter.hpp"

#include "duckdb.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pg/declarations.hpp"
#include "pgduckdb/pg/datum.hpp"
#include "pgduckdb/pgduckdb_detoast.hpp"


extern "C" {
// Exceptional include of PG Header which doesn't contain any function declaration
// only macro definitions and types.
#include "catalog/pg_type_d.h"
}

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.


namespace pgduckdb {

template <class T, class OP>
bool
TemplatedFilterOperation(const T &value, const duckdb::Value &constant) {
	return OP::Operation(value, constant.GetValueUnsafe<T>());
}

template <class OP>
bool
StringFilterOperation(const Datum value, const duckdb::Value &constant, bool is_bpchar) {
	if (value == (Datum)0 || constant.IsNull()) {
		return false; // Comparison to NULL always returns false.
	}

	bool should_free = false;
	const auto detoasted_value = DetoastPostgresDatum(reinterpret_cast<varlena *>(value), &should_free);

	/* bpchar adds zero padding so we need to read true len of bpchar */
	const auto detoasted_val_len = pg::GetDetoastedDatumLen(detoasted_value, is_bpchar);
	const auto detoasted_val = pg::GetDetoastedDatumVal(detoasted_value);

	const auto datum_sv = std::string_view(detoasted_val, detoasted_val_len);
	const auto val = duckdb::StringValue::Get(constant);
	const auto val_sv = std::string_view(val);
	const bool res = OP::Operation(datum_sv, val_sv);

	if (should_free) {
		duckdb_free(reinterpret_cast<void *>(detoasted_value));
	}
	return res;
}

template <class OP>
static bool
FilterOperationSwitch(const Datum &value, const duckdb::Value &constant, Oid type_oid) {
	switch (type_oid) {
	case BOOLOID:
		return TemplatedFilterOperation<bool, OP>(pg::DatumGetBool(value), constant);
	case CHAROID:
		return TemplatedFilterOperation<uint8_t, OP>(pg::DatumGetChar(value), constant);
	case INT2OID:
		return TemplatedFilterOperation<int16_t, OP>(pg::DatumGetInt16(value), constant);
	case INT4OID:
		return TemplatedFilterOperation<int32_t, OP>(pg::DatumGetInt32(value), constant);
	case INT8OID:
		return TemplatedFilterOperation<int64_t, OP>(pg::DatumGetInt64(value), constant);
	case FLOAT4OID:
		return TemplatedFilterOperation<float, OP>(pg::DatumGetFloat4(value), constant);
	case FLOAT8OID:
		return TemplatedFilterOperation<double, OP>(pg::DatumGetFloat8(value), constant);
	case DATEOID: {
		int32_t date = pg::DatumGetDateADT(value) + pgduckdb::PGDUCKDB_DUCK_DATE_OFFSET;
		return TemplatedFilterOperation<int32_t, OP>(date, constant);
	}
	case TIMESTAMPOID: {
		int64_t timestamp = pg::DatumGetTimestamp(value) + pgduckdb::PGDUCKDB_DUCK_TIMESTAMP_OFFSET;
		return TemplatedFilterOperation<int64_t, OP>(timestamp, constant);
	}
	case TIMESTAMPTZOID: {
		int64_t timestamptz = pg::DatumGetTimestampTz(value) + pgduckdb::PGDUCKDB_DUCK_TIMESTAMP_OFFSET;
		return TemplatedFilterOperation<int64_t, OP>(timestamptz, constant);
	}
	case BPCHAROID:
	case TEXTOID:
	case VARCHAROID:
		return StringFilterOperation<OP>(value, constant, type_oid == BPCHAROID);
	default:
		throw duckdb::InvalidTypeException(
		    duckdb::string("(DuckDB/FilterOperationSwitch) Unsupported duckdb type: " + std::to_string(type_oid)));
	}
}

bool
ApplyValueFilter(const duckdb::TableFilter &filter, const Datum &value, bool is_null, Oid type_oid) {
	switch (filter.filter_type) {
	case duckdb::TableFilterType::CONJUNCTION_AND: {
		const auto &conjunction = filter.Cast<duckdb::ConjunctionAndFilter>();
		for (const auto &child_filter : conjunction.child_filters) {
			if (!ApplyValueFilter(*child_filter, value, is_null, type_oid)) {
				return false;
			}
		}
		return true;
	}
	case duckdb::TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<duckdb::ConstantFilter>();
		switch (constant_filter.comparison_type) {
		case duckdb::ExpressionType::COMPARE_EQUAL:
			return FilterOperationSwitch<duckdb::Equals>(value, constant_filter.constant, type_oid);
		case duckdb::ExpressionType::COMPARE_LESSTHAN:
			return FilterOperationSwitch<duckdb::LessThan>(value, constant_filter.constant, type_oid);
		case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return FilterOperationSwitch<duckdb::LessThanEquals>(value, constant_filter.constant, type_oid);
		case duckdb::ExpressionType::COMPARE_GREATERTHAN:
			return FilterOperationSwitch<duckdb::GreaterThan>(value, constant_filter.constant, type_oid);
		case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return FilterOperationSwitch<duckdb::GreaterThanEquals>(value, constant_filter.constant, type_oid);
		default:
			D_ASSERT(0);
		}
		break;
	}
	case duckdb::TableFilterType::IS_NOT_NULL:
		return is_null == false;
	case duckdb::TableFilterType::IS_NULL:
		return is_null == true;
	default:
		D_ASSERT(0);
		break;
	}
}

} // namespace pgduckdb
