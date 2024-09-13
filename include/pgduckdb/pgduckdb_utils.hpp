#pragma once

extern "C" {
#include "postgres.h"
}

#include <vector>
#include <string>
#include <sstream>
#include <cstdio>
#include <optional>

namespace pgduckdb {

inline std::vector<std::string>
TokenizeString(char *str, const char delimiter) {
	std::vector<std::string> v;
	std::stringstream ss(str); // Turn the string into a stream.
	std::string tok;
	while (getline(ss, tok, delimiter)) {
		v.push_back(tok);
	}
	return v;
};

template <typename T, typename FuncType, typename... FuncArgs>
std::optional<T>
PostgresFunctionGuard(FuncType postgres_function, FuncArgs... args) {
	T return_value;
	bool error = false;
	// clang-format off
	PG_TRY();
	{
		return_value = postgres_function(args...);
	}
	PG_CATCH();
	{
		error = true;
	}
	PG_END_TRY();
	// clang-format on
	if (error) {
		return std::nullopt;
	}
	return return_value;
}

template <typename FuncType, typename... FuncArgs>
bool
PostgresVoidFunctionGuard(FuncType postgres_function, FuncArgs... args) {
	bool error = false;
	// clang-format off
	PG_TRY();
	{
		postgres_function(args...);
	}
	PG_CATCH();
	{
		error = true;
	}
	PG_END_TRY();
	// clang-format on
	return error;
}

} // namespace pgduckdb
