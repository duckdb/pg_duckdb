#pragma once

void DuckdbInitBackgroundWorker(void);

namespace pgduckdb {

void SyncMotherDuckCatalogsWithPg(bool drop_with_cascade);
extern bool doing_motherduck_sync;
extern char *current_duckdb_database_name;
extern char *current_motherduck_catalog_version;

} // namespace pgduckdb
