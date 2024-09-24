#pragma once

#include "pgduckdb/duckdb_vendor/enum_type_info_templated.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
}

namespace pgduckdb {

using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Vector;

// To store additional metadata for a type, DuckDB's LogicalType class contains a 'type_info_' field.
// The type of this is ExtraTypeInfo, ENUMs make use of this in the form of EnumTypeInfo (see
// duckdb/include/common/extra_type_info.hpp). We derive from this further to store the ENUMs connection with the oids
// of Postgres' enum members.

template <class T>
class PGDuckDBEnumTypeInfo : public duckdb::EnumTypeInfoTemplated<T> {
public:
	PGDuckDBEnumTypeInfo(Vector &values_insert_order_p, idx_t dict_size_p, Vector &enum_member_oids)
	    : duckdb::EnumTypeInfoTemplated<T>(values_insert_order_p, dict_size_p), enum_member_oids(enum_member_oids) {
	}

public:
	const Vector &
	GetMemberOids() const {
		return enum_member_oids;
	}

	duckdb::shared_ptr<duckdb::ExtraTypeInfo>
	Copy() const override {
		auto &insert_order = this->GetValuesInsertOrder();
		Vector values_insert_order_copy(LogicalType::VARCHAR, false, false, 0);
		values_insert_order_copy.Reference(insert_order);

		Vector enum_member_oids_copy(LogicalType::UINTEGER, false, false, 0);
		enum_member_oids_copy.Reference(enum_member_oids);

		return duckdb::make_shared_ptr<PGDuckDBEnumTypeInfo>(values_insert_order_copy, this->GetDictSize(),
		                                                     enum_member_oids_copy);
	}

private:
	Vector enum_member_oids;
};

struct PGDuckDBEnum {
	static LogicalType CreateEnumType(std::vector<HeapTuple> &enum_members);
	static idx_t GetDuckDBEnumPosition(duckdb::Value &val);
	static idx_t GetEnumPosition(Datum enum_member_oid, const duckdb::LogicalType &type);
	static bool IsEnumType(Oid type_oid);
	static Oid GetEnumTypeOid(const Vector &oids);
	static const Vector &GetMemberOids(const duckdb::LogicalType &type);
};

} // namespace pgduckdb
