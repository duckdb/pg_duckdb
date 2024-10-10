#pragma once

extern "C" {
#include "postgres.h"
}

#include "duckdb/common/exception.hpp"
#include "duckdb/common/error_data.hpp"

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
	}
	PG_END_TRY();
	// clang-format on
	if (edata) {
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, edata->message);
	}
	return return_value;
}

template <typename FuncType, typename... FuncArgs>
void
PostgresFunctionGuard(FuncType postgres_function, FuncArgs... args) {
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
	}
	PG_END_TRY();
	// clang-format on
	if (edata) {
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, edata->message);
	}
}

template <typename FuncRetT, typename FuncType, typename... FuncArgs>
FuncRetT
DuckDBFunctionGuard(FuncType duckdb_function, const char* function_name, FuncArgs... args) {
	const char *error_message = nullptr;
	try {
		return duckdb_function(args...);
	} catch (duckdb::Exception &ex) {
		duckdb::ErrorData edata(ex.what());
		error_message = pstrdup(edata.Message().c_str());
	} catch (std::exception &ex) {
		const auto msg = ex.what();
		if (msg[0] == '{') {
			duckdb::ErrorData edata(ex.what());
			error_message = pstrdup(edata.Message().c_str());
		} else {
			error_message = pstrdup(ex.what());
		}
	}

	if (error_message) {
		elog(ERROR, "(PGDuckDB/%s) %s", function_name, error_message);
	}

	std::abort(); // Cannot reach.
}

} // namespace pgduckdb
