#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/error_data.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"

extern "C" {
#include "postgres.h"
}

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
	pgduckdb::__PostgresFunctionGuard__<decltype(&FUNC), &FUNC>("##FUNC##", __VA_ARGS__)

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

std::string CreateOrGetDirectoryPath(std::string directory_name);

duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(duckdb::ClientContext &context, const std::string &query);

duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(duckdb::Connection &connection, const std::string &query);

duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(const std::string &query);

bool TryDuckDBQuery(duckdb::ClientContext &context, const std::string &query);
bool TryDuckDBQuery(const std::string &query);

} // namespace pgduckdb
