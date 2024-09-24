#pragma once

#include <vector>
#include <functional>
#include <string>
#include <utility> // std::pair

namespace pgduckdb_tests {

extern std::vector<std::pair<std::string, std::function<void()>>> PGDUCKDB_TESTS;

} // namespace pgduckdb_tests
