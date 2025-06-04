#pragma once

#include "pgduckdb/pg/declarations.hpp"

#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace pgduckdb {

class PgDuckLakeMetadataManager : public duckdb::DuckLakeMetadataManager {
public:
	PgDuckLakeMetadataManager(duckdb::DuckLakeTransaction &transaction);

	duckdb::DuckLakeCatalogInfo GetCatalogForSnapshot(duckdb::DuckLakeSnapshot snapshot) override;
	duckdb::vector<duckdb::DuckLakeGlobalStatsInfo> GetGlobalTableStats(duckdb::DuckLakeSnapshot snapshot) override;
	duckdb::vector<duckdb::DuckLakeFileListEntry> GetFilesForTable(duckdb::DuckLakeTableEntry &table_entry,
	                                                               duckdb::DuckLakeSnapshot snapshot,
	                                                               const duckdb::string &filter) override;
	duckdb::unique_ptr<duckdb::DuckLakeSnapshot> GetSnapshot() override;
	duckdb::unique_ptr<duckdb::DuckLakeSnapshot> GetSnapshot(duckdb::BoundAtClause &at_clause) override;

	void WriteNewSchemas(duckdb::DuckLakeSnapshot commit_snapshot,
	                     const duckdb::vector<duckdb::DuckLakeSchemaInfo> &new_schemas);
	void WriteNewTables(duckdb::DuckLakeSnapshot commit_snapshot,
	                    const duckdb::vector<duckdb::DuckLakeTableInfo> &new_tables) override;

	void InsertSnapshot(duckdb::DuckLakeSnapshot commit_snapshot) override;
	void WriteSnapshotChanges(duckdb::DuckLakeSnapshot commit_snapshot,
	                          const duckdb::SnapshotChangeInfo &change_info) override;
	void WriteNewDataFiles(duckdb::DuckLakeSnapshot commit_snapshot,
	                       const duckdb::vector<duckdb::DuckLakeFileInfo> &new_files) override;
	void UpdateGlobalTableStats(const duckdb::DuckLakeGlobalStatsInfo &stats) override;

	bool GetDuckLakeTableInfo(const duckdb::DuckLakeSnapshot &snapshot, duckdb::DuckLakeSchemaEntry &schema,
	                          duckdb::DuckLakeTableInfo &table_info);

private:
	void WriteNewSchema(duckdb::DuckLakeSnapshot commit_snapshot, const duckdb::DuckLakeSchemaInfo &schema_info);
	void WriteNewTable(duckdb::DuckLakeSnapshot commit_snapshot, const duckdb::DuckLakeTableInfo &table_info);
	int GetDuckLakeSchemas(const duckdb::DuckLakeSnapshot &snapshot, duckdb::DuckLakeCatalogInfo &catalog_info);
	int GetDuckLakeTables(const duckdb::DuckLakeSnapshot &snapshot, duckdb::DuckLakeCatalogInfo &catalog);

	Snapshot snapshot;
};

} // namespace pgduckdb
