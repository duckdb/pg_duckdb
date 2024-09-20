#include "pgduckdb/types/pgduckdb_enum.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "duckdb/common/extra_type_info.hpp"

namespace pgduckdb {

using duckdb::idx_t;
using duckdb::LogicalTypeId;
using duckdb::PhysicalType;

idx_t
PGDuckDBEnum::GetDuckDBEnumPosition(duckdb::Value &val) {
	D_ASSERT(val.type().id() == LogicalTypeId::ENUM);
	auto physical_type = val.type().InternalType();
	switch (physical_type) {
	case PhysicalType::UINT8:
		return val.GetValue<uint8_t>();
	case PhysicalType::UINT16:
		return val.GetValue<uint16_t>();
	case PhysicalType::UINT32:
		return val.GetValue<uint32_t>();
	default:
		throw duckdb::InternalException("Invalid Physical Type for ENUMs");
	}
}

idx_t
PGDuckDBEnum::GetEnumPosition(Datum enum_member_oid_p, const duckdb::LogicalType &type) {
	auto enum_member_oid = DatumGetObjectId(enum_member_oid_p);

	auto &enum_type_info = type.AuxInfo()->Cast<duckdb::EnumTypeInfo>();
	auto dict_size = enum_type_info.GetDictSize();

	auto enum_member_oids = PGDuckDBEnum::GetMemberOids(type);
	auto oids_data = duckdb::FlatVector::GetData<uint32_t>(enum_member_oids);

	uint32_t *begin = oids_data;
	uint32_t *end = oids_data + dict_size;
	uint32_t *result = std::find(begin, end, enum_member_oid);

	if (result == end) {
		throw duckdb::InternalException("Could not find enum_member_oid: %d", enum_member_oid);
	}
	return (idx_t)(result - begin);
}

bool
PGDuckDBEnum::IsEnumType(Oid type_oid) {
	bool result = false;
	auto type_tuple = PostgresFunctionGuard<HeapTuple>(SearchSysCache1, TYPEOID, ObjectIdGetDatum(type_oid));

	if (HeapTupleIsValid(type_tuple)) {
		auto type_form = (Form_pg_type)GETSTRUCT(type_tuple);

		// Check if the type is an enum
		if (type_form->typtype == 'e') {
			result = true;
		}
		PostgresFunctionGuard(ReleaseSysCache, type_tuple);
	}
	return result;
}

Oid
PGDuckDBEnum::GetEnumTypeOid(const Vector &oids) {
	/* Get the pg_type tuple for the enum type */
	auto member_oid = duckdb::FlatVector::GetData<uint32_t>(oids)[0];
	auto tuple = PostgresFunctionGuard<HeapTuple>(SearchSysCache1, ENUMOID, ObjectIdGetDatum(member_oid));
	Oid result = InvalidOid;
	if (!HeapTupleIsValid(tuple)) {
		throw duckdb::InvalidInputException("Cache lookup failed for enum member with oid %d", member_oid);
	}

	auto enum_form = (Form_pg_enum)GETSTRUCT(tuple);
	result = enum_form->enumtypid;

	/* Release the cache tuple */
	PostgresFunctionGuard(ReleaseSysCache, tuple);
	return result;
}

Vector
PGDuckDBEnum::GetMemberOids(const duckdb::LogicalType &type) {
	auto type_info = type.AuxInfo();
	auto &enum_type_info = type_info->Cast<duckdb::EnumTypeInfo>();
	auto &enum_sort_order = enum_type_info.GetValuesInsertOrder();
	idx_t dict_size = enum_type_info.GetDictSize();

	// We store the (Postgres) enum member oids behind the string data in the EnumTypeInfo
	auto vector_data = duckdb::FlatVector::GetData<duckdb::string_t>(enum_sort_order);
	// Create a Vector that wraps the existing data, without copying - only valid while the EnumTypeInfo is alive
	return Vector(duckdb::LogicalType::UINTEGER, (duckdb::data_ptr_t)(vector_data + dict_size));
}

} // namespace pgduckdb
