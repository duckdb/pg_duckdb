#include "duckdb.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/pg_type.h"
}

#include "quack/quack_filter.hpp"
#include "quack/quack_types.hpp"

namespace quack {

template <class T, class OP>
bool
TemplatedFilterOperation(Datum &value, const duckdb::Value &constant) {
	return OP::Operation((T)value, constant.GetValueUnsafe<T>());
}

template <class OP>
static bool
FilterOperationSwitch(Datum &value, duckdb::Value &constant, Oid typeOid) {
	switch (typeOid) {
	case BOOLOID:
		return TemplatedFilterOperation<bool, OP>(value, constant);
		break;
	case CHAROID:
		return TemplatedFilterOperation<uint8_t, OP>(value, constant);
		break;
	case INT2OID:
		return TemplatedFilterOperation<int16_t, OP>(value, constant);
		break;
	case INT4OID:
		return TemplatedFilterOperation<int32_t, OP>(value, constant);
		break;
	case INT8OID:
		return TemplatedFilterOperation<int64_t, OP>(value, constant);
		break;
	case FLOAT4OID:
		return TemplatedFilterOperation<float, OP>(value, constant);
		break;
	case FLOAT8OID:
		return TemplatedFilterOperation<double, OP>(value, constant);
		break;
	case DATEOID: {
		Datum dateDatum = static_cast<int32_t>(value + quack::QUACK_DUCK_DATE_OFFSET);
		return TemplatedFilterOperation<int32_t, OP>(dateDatum, constant);
		break;
	}
	default:
		elog(ERROR, "(DuckDB/FilterOperationSwitch) Unsupported quack type: %d", typeOid);
	}
}

bool
ApplyValueFilter(duckdb::TableFilter &filter, Datum &value, bool isNull, Oid typeOid) {
	switch (filter.filter_type) {
	case duckdb::TableFilterType::CONJUNCTION_AND: {
		auto &conjunction = filter.Cast<duckdb::ConjunctionAndFilter>();
		bool valueFilterResult = true;
		for (auto &child_filter : conjunction.child_filters) {
			valueFilterResult &= ApplyValueFilter(*child_filter, value, isNull, typeOid);
		}
		return valueFilterResult;
		break;
	}
	case duckdb::TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<duckdb::ConstantFilter>();
		switch (constant_filter.comparison_type) {
		case duckdb::ExpressionType::COMPARE_EQUAL:
			return FilterOperationSwitch<duckdb::Equals>(value, constant_filter.constant, typeOid);
			break;
		case duckdb::ExpressionType::COMPARE_LESSTHAN:
			return FilterOperationSwitch<duckdb::LessThan>(value, constant_filter.constant, typeOid);
			break;
		case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return FilterOperationSwitch<duckdb::LessThanEquals>(value, constant_filter.constant, typeOid);
			break;
		case duckdb::ExpressionType::COMPARE_GREATERTHAN:
			return FilterOperationSwitch<duckdb::GreaterThan>(value, constant_filter.constant, typeOid);
			break;
		case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return FilterOperationSwitch<duckdb::GreaterThanEquals>(value, constant_filter.constant, typeOid);
			break;
		default:
			D_ASSERT(0);
		}
		break;
	}
	case duckdb::TableFilterType::IS_NOT_NULL:
		return isNull == false;
		break;
	case duckdb::TableFilterType::IS_NULL:
		return isNull == true;
		break;
	default:
		D_ASSERT(0);
		break;
	}
}

} // namespace quack