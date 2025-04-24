#pragma once

#include <duckdb/common/error_data.hpp>

extern "C" {
#include "postgres.h"
}

namespace pgduckdb {

template <typename Func, Func func, typename... FuncArgs>
typename std::invoke_result<Func, FuncArgs &...>::type
__CPPFunctionGuard__(const char *func_name, FuncArgs &...args) {
	const char *error_message = nullptr;
	auto pg_es_start = PG_exception_stack;
	try {
		return func(args...);
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

	if (pg_es_start != PG_exception_stack) {
		elog(WARNING, "WARNING: Unexpected exception stack pointer. This is not expected, please report this.");
		PG_exception_stack = pg_es_start;
	}

	elog(ERROR, "(PGDuckDB/%s) %s", func_name, error_message);
}

} // namespace pgduckdb

#define InvokeCPPFunc(FUNC, ...) pgduckdb::__CPPFunctionGuard__<decltype(&FUNC), &FUNC>(#FUNC, ##__VA_ARGS__)

// Wrappers

#define DECLARE_PG_FUNCTION(func_name)                                                                                 \
	PG_FUNCTION_INFO_V1(func_name);                                                                                    \
	Datum func_name##_cpp(PG_FUNCTION_ARGS);                                                                           \
	Datum func_name(PG_FUNCTION_ARGS) {                                                                                \
		return InvokeCPPFunc(func_name##_cpp, fcinfo);                                                                 \
	}                                                                                                                  \
	Datum func_name##_cpp(PG_FUNCTION_ARGS __attribute__((unused)))
