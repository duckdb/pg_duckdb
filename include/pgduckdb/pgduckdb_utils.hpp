#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/error_data.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pg/error_data.hpp"
#include "pgduckdb/logger.hpp"

#include <setjmp.h>

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

extern "C" {
// Note: these forward-declarations could live in a header under the `pg/` folder.
// But since they are (hopefully) only used in this file, we keep them here.
struct ErrorContextCallback;
struct MemoryContextData;

typedef struct MemoryContextData *MemoryContext;
typedef char *pg_stack_base_t;

extern sigjmp_buf *PG_exception_stack;
extern MemoryContext CurrentMemoryContext;
extern ErrorContextCallback *error_context_stack;
extern ErrorData *CopyErrorData();
extern void FlushErrorState();
extern pg_stack_base_t set_stack_base();
extern void restore_stack_base(pg_stack_base_t base);
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
 * PostgresScopedStackReset is a RAII class that saves the current stack base
 * and restores it on destruction. When calling certain Postgres C functions
 * from other threads than the main thread this is necessary to avoid Postgres
 * throwing an error running out of stack space. In codepaths that postgres
 * expects to be called recursively it checks if the stack size is still within
 * the limit set by max_stack_depth. It does so by comparing the current stack
 * pointer to the pointer it saved when starting the process. But since
 * different threads have different stacks, this check will fail basically
 * automatically if the thread is not the main thread. This class is a
 * workaround for this problem, by configuring a new stack base matching the
 * current location of the stack. This does mean that the stack might grow
 * higher than, but for our use case this shouldn't matter anyway because we
 * don't expect any recursive functions to be called. And even if we did expect
 * that, the default max_stack_depth is conservative enough to handle this small
 * bit of extra stack space.
 */
struct PostgresScopedStackReset {
	PostgresScopedStackReset() {
		saved_current_stack = set_stack_base();
	}
	~PostgresScopedStackReset() {
		restore_stack_base(saved_current_stack);
	}
	pg_stack_base_t saved_current_stack;
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
			CurrentMemoryContext = ctx;
			edata = CopyErrorData();
			FlushErrorState();
		}
	} // PG_END_TRY();

	auto message = duckdb::StringUtil::Format("(PGDuckDB/%s) %s", func_name, pg::GetErrorDataMessage(edata));
	throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, message);
}

#define PostgresFunctionGuard(FUNC, ...)                                                                               \
	pgduckdb::__PostgresFunctionGuard__<decltype(&FUNC), &FUNC>(__func__, ##__VA_ARGS__)

duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(duckdb::ClientContext &context, const std::string &query);

duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(duckdb::Connection &connection, const std::string &query);

duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(const std::string &query);

} // namespace pgduckdb
