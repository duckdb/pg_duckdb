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
	PGDuckDBEnumTypeInfo(Vector &values_insert_order_p, idx_t dict_size_p, Vector &oid_vec)
	    : EnumTypeInfoTemplated<T>(values_insert_order_p, dict_size_p), oid_vec(oid_vec) {
	}

public:
	const Vector &GetMemberOids() const {
		return oid_vec;
	}

private:
	Vector oid_vec;
};

struct PGDuckDBEnum {
	static LogicalType
	CreateEnumType(Vector &ordered_data, idx_t size, Vector &oid_vec) {
		// Generate EnumTypeInfo
		shared_ptr<ExtraTypeInfo> info;
		auto enum_internal_type = EnumTypeInfo::DictType(size);
		switch (enum_internal_type) {
		case PhysicalType::UINT8:
			info = make_shared_ptr<PGDuckDBEnumTypeInfo<uint8_t>>(ordered_data, size, oid_vec);
			break;
		case PhysicalType::UINT16:
			info = make_shared_ptr<PGDuckDBEnumTypeInfo<uint16_t>>(ordered_data, size, oid_vec);
			break;
		case PhysicalType::UINT32:
			info = make_shared_ptr<PGDuckDBEnumTypeInfo<uint32_t>>(ordered_data, size, oid_vec);
			break;
		default:
			throw InternalException("Invalid Physical Type for ENUMs");
		}
		// Generate Actual Enum Type
		return LogicalType(LogicalTypeId::ENUM, info);
	}
};

} // namespace pgduckdb
