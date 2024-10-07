#pragma once

extern "C" {
#include "postgres.h"
}

#include "duckdb/common/exception.hpp"

#include <vector>
#include <string>
#include <sstream>

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

/*
 * DuckdbGlobalLock should be held before calling.
 */
template <typename T, typename FuncType, typename... FuncArgs>
T
PostgresFunctionGuard(FuncType postgres_function, FuncArgs... args) {
	T return_value;
	bool error = false;
	MemoryContext ctx = CurrentMemoryContext;
	ErrorData *edata = nullptr;
	// clang-format off
	PG_TRY();
	{
		return_value = postgres_function(args...);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(ctx);
		edata = CopyErrorData();
		FlushErrorState();
		error = true;
	}
	PG_END_TRY();
	// clang-format on
	if (error) {
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, edata->message);
	}
	return return_value;
}

template <typename FuncType, typename... FuncArgs>
void
PostgresFunctionGuard(FuncType postgres_function, FuncArgs... args) {
	bool error = false;
	MemoryContext ctx = CurrentMemoryContext;
	ErrorData *edata = nullptr;
	// clang-format off
	PG_TRY();
	{
		postgres_function(args...);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(ctx);
		edata = CopyErrorData();
		FlushErrorState();
		error = true;
	}
	PG_END_TRY();
	// clang-format on
	if (error) {
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, edata->message);
	}
}

} // namespace pgduckdb
