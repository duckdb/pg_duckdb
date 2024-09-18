#pragma once

#include "pgduckdb/duckdb_vendor/enum_type_info_templated.hpp"

namespace pgduckdb {

using duckdb::EnumTypeInfo;
using duckdb::EnumTypeInfoTemplated;
using duckdb::ExtraTypeInfo;
using duckdb::InternalException;
using duckdb::LogicalType;
using duckdb::LogicalTypeId;
using duckdb::make_shared_ptr;
using duckdb::PhysicalType;
using duckdb::shared_ptr;
using duckdb::Vector;

template <class T>
class PGDuckDBEnumTypeInfo : public EnumTypeInfoTemplated<T> {
public:
	PGDuckDBEnumTypeInfo(Vector &values_insert_order_p, idx_t dict_size_p, Oid type_oid)
	    : EnumTypeInfoTemplated<T>(values_insert_order_p, dict_size_p), type_oid(type_oid) {
	}

public:
	Oid
	GetTypeOid() const {
		return type_oid;
	}

private:
	Oid type_oid = InvalidOid;
};

struct PGDuckDBEnum {
	static LogicalType
	CreateEnumType(Vector &ordered_data, idx_t size, Oid type_oid) {
		// Generate EnumTypeInfo
		shared_ptr<ExtraTypeInfo> info;
		auto enum_internal_type = EnumTypeInfo::DictType(size);
		switch (enum_internal_type) {
		case PhysicalType::UINT8:
			info = make_shared_ptr<PGDuckDBEnumTypeInfo<uint8_t>>(ordered_data, size, type_oid);
			break;
		case PhysicalType::UINT16:
			info = make_shared_ptr<PGDuckDBEnumTypeInfo<uint16_t>>(ordered_data, size, type_oid);
			break;
		case PhysicalType::UINT32:
			info = make_shared_ptr<PGDuckDBEnumTypeInfo<uint32_t>>(ordered_data, size, type_oid);
			break;
		default:
			throw InternalException("Invalid Physical Type for ENUMs");
		}
		// Generate Actual Enum Type
		return LogicalType(LogicalTypeId::ENUM, info);
	}
};

} // namespace pgduckdb
