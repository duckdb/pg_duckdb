#pragma once

#include "duckdb/common/exception.hpp"

extern "C" {
struct ErrorData;
}

namespace pgduckdb {

/*
Custom exception class for PGDuckDB that acts as a shell for ErrorData.

Note: we have to capture the ErrorData pointer in the exception which is
a subclass of duckdb::Exception. This is because in some cases, the exception
is caught by DuckDB and re-thrown. So the original exception is lost.

While it is not ideal to stringify the pointer, it should be valid as long as
it is properly caught by the CPP exception handler in an upper stack.
*/

struct Exception : public duckdb::Exception {
	Exception(ErrorData *edata = nullptr)
	    : duckdb::Exception::Exception(duckdb::ExceptionType::INVALID, "PGDuckDB Exception",
	                                   {{"error_data_ptr", std::to_string(reinterpret_cast<uintptr_t>(edata))}}),
	      error_data(edata) {
	}

	ErrorData *error_data = nullptr;

private:
	Exception(const Exception &) = delete;
	Exception &operator=(const Exception &) = delete;
};

} // namespace pgduckdb
