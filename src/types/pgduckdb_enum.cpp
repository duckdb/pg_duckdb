#include "pgduckdb/types/pgduckdb_enum.hpp"

namespace pgduckdb {

LogicalType PGDuckDBEnum::CreateEnumType(Vector &ordered_data, idx_t size, Vector &enum_member_oids) {
	// Generate EnumTypeInfo
	shared_ptr<ExtraTypeInfo> info;
	auto enum_internal_type = EnumTypeInfo::DictType(size);
	switch (enum_internal_type) {
	case PhysicalType::UINT8:
		info = make_shared_ptr<PGDuckDBEnumTypeInfo<uint8_t>>(ordered_data, size, enum_member_oids);
		break;
	case PhysicalType::UINT16:
		info = make_shared_ptr<PGDuckDBEnumTypeInfo<uint16_t>>(ordered_data, size, enum_member_oids);
		break;
	case PhysicalType::UINT32:
		info = make_shared_ptr<PGDuckDBEnumTypeInfo<uint32_t>>(ordered_data, size, enum_member_oids);
		break;
	default:
		throw InternalException("Invalid Physical Type for ENUMs");
	}
	// Generate Actual Enum Type
	return LogicalType(LogicalTypeId::ENUM, info);
}

idx_t PGDuckDBEnum::GetDuckDBEnumPosition(duckdb::Value &val) {
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
		throw InternalException("Invalid Physical Type for ENUMs");
	}
}

int PGDuckDBEnum::GetEnumPosition(Datum enum_member_oid) {
	HeapTuple enum_tuple;
	Form_pg_enum enum_entry;
	int enum_position;

	// Use SearchSysCache1 to fetch the enum entry from pg_enum using its OID
	enum_tuple = SearchSysCache1(ENUMOID, enum_member_oid);

	if (!HeapTupleIsValid(enum_tuple)) {
		elog(ERROR, "could not find enum with OID %u", DatumGetObjectId(enum_member_oid));
	}

	// Get the pg_enum structure from the tuple
	enum_entry = (Form_pg_enum)GETSTRUCT(enum_tuple);

	// The enumsortorder field gives us the position of the enum value
	enum_position = (int)enum_entry->enumsortorder;

	// Release the cache to free up memory
	ReleaseSysCache(enum_tuple);

	return enum_position;
}

bool PGDuckDBEnum::IsEnumType(Oid type_oid) {
	bool result = false;
	auto type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));

	if (HeapTupleIsValid(type_tuple)) {
		auto type_form = (Form_pg_type)GETSTRUCT(type_tuple);

		// Check if the type is an enum
		if (type_form->typtype == 'e') {
			result = true;
		}
		ReleaseSysCache(type_tuple);
	}
	return result;
}

Oid PGDuckDBEnum::GetEnumTypeOid(const Vector &oids) {
	/* Get the pg_type tuple for the enum type */
	auto member_oid = duckdb::FlatVector::GetData<uint32_t>(oids)[0];
	auto tuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(member_oid));
	Oid result = InvalidOid;
	if (!HeapTupleIsValid(tuple)) {
		elog(ERROR, "cache lookup failed for enum member with oid %u", member_oid);
	}

	auto enum_form = (Form_pg_enum)GETSTRUCT(tuple);
	result = enum_form->enumtypid;

	/* Release the cache tuple */
	ReleaseSysCache(tuple);
	return result;
}

const Vector &PGDuckDBEnum::GetMemberOids(const duckdb::LogicalType &type) {
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
		throw InternalException("Invalid Physical Type for ENUMs");
	}
}

} // namespace pgduckdb
