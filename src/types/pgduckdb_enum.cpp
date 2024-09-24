#include "pgduckdb/types/pgduckdb_enum.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "duckdb/common/extra_type_info.hpp"

namespace pgduckdb {

using duckdb::idx_t;
using duckdb::LogicalTypeId;
using duckdb::PhysicalType;

// This is medium hacky, we create extra space in the Vector that would normally only hold the strings corresponding to
// the enum's members. In that extra space we store the enum member oids.

// We do this so we can find the OID from the offset (which is what DuckDB stores in a Vector of type ENUM) and return
// that when converting the result from DuckDB -> Postgres

LogicalType
PGDuckDBEnum::CreateEnumType(std::vector<HeapTuple> &enum_members) {
	auto size = enum_members.size();

	auto duck_enum_vec = duckdb::Vector(duckdb::LogicalType::VARCHAR, size);
	auto enum_member_oid_vec = duckdb::Vector(duckdb::LogicalType::UINTEGER, size);
	auto enum_vec_data = duckdb::FlatVector::GetData<duckdb::string_t>(duck_enum_vec);
	auto enum_member_oid_data = duckdb::FlatVector::GetData<uint32_t>(enum_member_oid_vec);
	for (idx_t i = 0; i < size; i++) {
		auto &member = enum_members[i];
		auto enum_data = (Form_pg_enum)GETSTRUCT(member);
		enum_vec_data[i] = duckdb::StringVector::AddString(duck_enum_vec, enum_data->enumlabel.data);
		enum_member_oid_data[i] = enum_data->oid;
	}

	// Generate EnumTypeInfo
	duckdb::shared_ptr<duckdb::ExtraTypeInfo> info;
	auto enum_internal_type = duckdb::EnumTypeInfo::DictType(size);
	switch (enum_internal_type) {
	case PhysicalType::UINT8:
		info = duckdb::make_shared_ptr<PGDuckDBEnumTypeInfo<uint8_t>>(duck_enum_vec, size, enum_member_oid_vec);
		break;
	case PhysicalType::UINT16:
		info = duckdb::make_shared_ptr<PGDuckDBEnumTypeInfo<uint16_t>>(duck_enum_vec, size, enum_member_oid_vec);
		break;
	case PhysicalType::UINT32:
		info = duckdb::make_shared_ptr<PGDuckDBEnumTypeInfo<uint32_t>>(duck_enum_vec, size, enum_member_oid_vec);
		break;
	default:
		throw duckdb::InternalException("Invalid Physical Type for ENUMs");
	}
	// Generate Actual Enum Type
	return LogicalType(LogicalTypeId::ENUM, info);
}

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

	auto &enum_member_oids = PGDuckDBEnum::GetMemberOids(type);
	auto oids_data = duckdb::FlatVector::GetData<uint32_t>(enum_member_oids);

	const uint32_t *begin = oids_data;
	const uint32_t *end = oids_data + dict_size;
	const uint32_t *result = std::find(begin, end, enum_member_oid);

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

const Vector &
PGDuckDBEnum::GetMemberOids(const duckdb::LogicalType &type) {
	auto type_info = type.AuxInfo();
	auto &enum_type_info = type_info->Cast<duckdb::EnumTypeInfo>();

	switch (enum_type_info.DictType(enum_type_info.GetDictSize())) {
	case PhysicalType::UINT8:
		return enum_type_info.Cast<PGDuckDBEnumTypeInfo<uint8_t>>().GetMemberOids();
	case PhysicalType::UINT16:
		return enum_type_info.Cast<PGDuckDBEnumTypeInfo<uint16_t>>().GetMemberOids();
	case PhysicalType::UINT32:
		return enum_type_info.Cast<PGDuckDBEnumTypeInfo<uint32_t>>().GetMemberOids();
	default:
		throw duckdb::InternalException("Invalid Physical Type for ENUMs");
	}
}

} // namespace pgduckdb
