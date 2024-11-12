#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/error_data.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"

extern "C" {
#include "postgres.h"
}

namespace pgduckdb {

struct PgExceptionGuard {
	PgExceptionGuard() : _save_exception_stack(PG_exception_stack), _save_context_stack(error_context_stack) {
	}

	~PgExceptionGuard() noexcept {
		RestoreStacks();
	}

	void
	RestoreStacks() const noexcept {
		PG_exception_stack = _save_exception_stack;
		error_context_stack = _save_context_stack;
	}

	sigjmp_buf *_save_exception_stack;
	ErrorContextCallback *_save_context_stack;
};

/*
 * DuckdbGlobalLock should be held before calling.
 */
template <typename Func, Func func, typename... FuncArgs>
typename std::invoke_result<Func, FuncArgs...>::type
__PostgresFunctionGuard__(const char *func_name, FuncArgs... args) {
	MemoryContext ctx = CurrentMemoryContext;
	ErrorData *edata = nullptr;
	{ // Scope for PG_END_TRY
		PgExceptionGuard g;
		sigjmp_buf _local_sigjmp_buf;
		if (sigsetjmp(_local_sigjmp_buf, 0) == 0) {
			PG_exception_stack = &_local_sigjmp_buf;
			return func(std::forward<FuncArgs>(args)...);
		} else {
			g.RestoreStacks();
			MemoryContextSwitchTo(ctx);
			edata = CopyErrorData();
			FlushErrorState();
		}
	} // PG_END_TRY();

	auto message = duckdb::StringUtil::Format("(PGDuckDB/%s) %s", func_name, edata->message);
	throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, message);
}

#define PostgresFunctionGuard(FUNC, ...)                                                                               \
	pgduckdb::__PostgresFunctionGuard__<decltype(&FUNC), &FUNC>(__func__, __VA_ARGS__)

template <typename Func, Func func, typename... FuncArgs>
typename std::invoke_result<Func, FuncArgs...>::type
__CPPFunctionGuard__(const char *func_name, FuncArgs... args) {
	const char *error_message = nullptr;
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

	elog(ERROR, "(PGDuckDB/%s) %s", func_name, error_message);
}

#define InvokeCPPFunc(FUNC, ...) pgduckdb::__CPPFunctionGuard__<decltype(&FUNC), &FUNC>(__FUNCTION__, __VA_ARGS__)

// Wrappers

#define DECLARE_PG_FUNCTION(func_name)                                                                                 \
	PG_FUNCTION_INFO_V1(func_name);                                                                                    \
	Datum func_name##_cpp(PG_FUNCTION_ARGS);                                                                           \
	Datum func_name(PG_FUNCTION_ARGS) {                                                                                \
		return InvokeCPPFunc(func_name##_cpp, fcinfo);                                                                 \
	} \
	Datum func_name##_cpp(PG_FUNCTION_ARGS)


duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(duckdb::ClientContext &context, const std::string &query);

duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(duckdb::Connection &connection, const std::string &query);

duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(const std::string &query);

} // namespace pgduckdb
