#pragma once

#include <string>
#include <vector>

namespace pgduckdb {

/* constants for duckdb.extensions */
#define Natts_duckdb_extension           3
#define Anum_duckdb_extension_name       1
#define Anum_duckdb_extension_enable     2
#define Anum_duckdb_extension_repository 3

typedef struct DuckdbExension {
	std::string name;
	bool enabled;
	std::string repository;
} DuckdbExtension;

extern std::vector<DuckdbExtension> ReadDuckdbExtensions();
extern std::string DuckdbInstallExtensionQuery(const std::string &extension_name, const std::string &repository);

} // namespace pgduckdb
