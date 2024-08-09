#pragma once

#include "duckdb.hpp"

#include <string>
#include <vector>

namespace pgduckdb {

void AddSecretsToDuckdbContext(duckdb::ClientContext &context);

typedef struct DuckdbExtension {
	std::string name;
	bool enabled;
} DuckdbExtension;

extern std::vector<DuckdbExtension> ReadDuckdbExtensions();

} // namespace pgduckdb
