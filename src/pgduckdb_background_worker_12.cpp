#include "pgduckdb/pgduckdb_background_worker.hpp"

extern "C" {
#include "postgres.h"
#include "fmgr.h"
}

namespace pgduckdb {

bool is_background_worker = false;
bool doing_motherduck_sync = false;
char *current_duckdb_database_name = nullptr;
char *current_motherduck_catalog_version = nullptr;

void InitBackgroundWorkersShmem(void) {
}

void StartBackgroundWorkerIfNeeded(void) {
}

void TriggerActivity(void) {
}

void UnclaimBgwSessionHint(int, Datum) {
}

const char *PossiblyReuseBgwSessionHint(void) {
	return nullptr;
}

} // namespace pgduckdb

extern "C" {
PG_FUNCTION_INFO_V1(force_motherduck_sync);

Datum
force_motherduck_sync(PG_FUNCTION_ARGS) {
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	                errmsg("force_motherduck_sync is not supported on PostgreSQL 12")));
	PG_RETURN_VOID();
}
}
