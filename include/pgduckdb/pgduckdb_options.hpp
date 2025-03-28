#pragma once

#include <string>
#include <vector>

namespace pgduckdb {

/* constants for duckdb.extensions */
#define Natts_duckdb_extension       2
#define Anum_duckdb_extension_name   1
#define Anum_duckdb_extension_enable 2

typedef struct DuckdbExension {
	std::string name;
	bool enabled;
} DuckdbExtension;

extern std::vector<DuckdbExtension> ReadDuckdbExtensions();

} // namespace pgduckdb
