#pragma once

extern "C" {
struct ErrorData;
}

namespace pgduckdb {
const char* GetErrorDataMessage(ErrorData* error_data);
}
