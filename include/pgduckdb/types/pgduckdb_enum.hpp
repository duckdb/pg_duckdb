#pragma once

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

using duckdb::LogicalType;
using duckdb::Vector;
using duckdb::idx_t;

struct PGDuckDBEnum {
	static idx_t GetDuckDBEnumPosition(duckdb::Value &val);
	static idx_t GetEnumPosition(Datum enum_member_oid, const duckdb::LogicalType &type);
	static bool IsEnumType(Oid type_oid);
	static Oid GetEnumTypeOid(const Vector &oids);
	static Vector GetMemberOids(const duckdb::LogicalType &type);
};

} // namespace pgduckdb
