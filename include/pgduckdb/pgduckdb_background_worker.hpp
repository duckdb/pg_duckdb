#pragma once

namespace pgduckdb {

void StartBackgroundWorkerIfNeeded(void);
void TriggerActivity(void);

extern bool is_background_worker;
extern bool doing_motherduck_sync;
extern char *current_duckdb_database_name;
extern char *current_motherduck_catalog_version;

} // namespace pgduckdb
