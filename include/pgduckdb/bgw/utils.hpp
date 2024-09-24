#pragma once

namespace pgduckdb {

extern "C" {

// TODO: config?
extern const char *PGDUCKDB_SOCKET_PATH;

extern const char *GetStrError();

extern int GetErrno();

extern void ELogError(const char *comp, const char *func_name);
} // extern "C"

} // namespace pgduckdb
