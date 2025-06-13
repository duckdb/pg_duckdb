#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

class PgDuckLakeTable {
public:
	static void CreateTable(Relation rel);
};

} // namespace pgduckdb
