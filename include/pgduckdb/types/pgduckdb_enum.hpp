#pragma once

#include "pgduckdb/duckdb_vendor/enum_type_info_templated.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
}

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

// To store additional metadata for a type, DuckDB's LogicalType class contains a 'type_info_' field.
// The type of this is ExtraTypeInfo, ENUMs make use of this in the form of EnumTypeInfo (see
// duckdb/include/common/extra_type_info.hpp). We derive from this further to store the ENUMs connection with the oids
// of Postgres' enum members.

template <class T>
class PGDuckDBEnumTypeInfo : public EnumTypeInfoTemplated<T> {
public:
	PGDuckDBEnumTypeInfo(Vector &values_insert_order_p, idx_t dict_size_p, Vector &enum_member_oids)
	    : EnumTypeInfoTemplated<T>(values_insert_order_p, dict_size_p), enum_member_oids(enum_member_oids) {
	}

public:
	const Vector &
	GetMemberOids() const {
		return enum_member_oids;
	}

private:
	Vector enum_member_oids;
};

struct PGDuckDBEnum {
	static LogicalType CreateEnumType(Vector &ordered_data, idx_t size, Vector &enum_member_oids);
	static idx_t GetDuckDBEnumPosition(duckdb::Value &val);
	static int GetEnumPosition(Datum enum_member_oid);
	static bool IsEnumType(Oid type_oid);
	static Oid GetEnumTypeOid(const Vector &oids);
	static const Vector &GetMemberOids(const duckdb::LogicalType &type);
};

} // namespace pgduckdb
