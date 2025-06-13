#include "pgducklake/pgducklake_metadata_manager.hpp"

#include "pgduckdb/logger.hpp"

#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"

extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

namespace pgduckdb {

namespace {

Oid
DuckDbNamespace(bool missing_ok = true) {
	return get_namespace_oid("duckdb", missing_ok);
}

Oid
DuckLakeSnapshotOid() {
	return get_relname_relid("ducklake_snapshot", DuckDbNamespace());
}

Oid
DuckLakeSchemaOid() {
	return get_relname_relid("ducklake_schema", DuckDbNamespace());
}

Oid
DuckLakeSnapshotIdOid() {
	return get_relname_relid("ducklake_snapshot_pkey", DuckDbNamespace());
}

Oid
DuckLakeSnapshotChangesOid() {
	return get_relname_relid("ducklake_snapshot_changes", DuckDbNamespace());
}

Oid
DuckLakeTableOid() {
	return get_relname_relid("ducklake_table", DuckDbNamespace());
}

Oid
DuckLakeColumnOid() {
	return get_relname_relid("ducklake_column", DuckDbNamespace());
}

Oid
DuckLakeDataFileOid() {
	return get_relname_relid("ducklake_data_file", DuckDbNamespace());
}
#if 0
Oid
DuckLakeDeleteFileOid() {
	return get_relname_relid("ducklake_delete_file", DuckDbNamespace());
}
#endif
} // namespace

PgDuckLakeMetadataManager::PgDuckLakeMetadataManager(duckdb::DuckLakeTransaction &transaction)
    : duckdb::DuckLakeMetadataManager(transaction) {
}

int
PgDuckLakeMetadataManager::GetDuckLakeSchemas(const duckdb::DuckLakeSnapshot &snapshot,
                                              duckdb::DuckLakeCatalogInfo &catalog_info) {
	::Relation table = table_open(DuckLakeSchemaOid(), AccessShareLock);
	TupleDesc desc = RelationGetDescr(table);
	ScanKeyData key[1];
	ScanKeyInit(&key[0], 3 /*begin_snapshot*/, BTLessEqualStrategyNumber, F_INT8LE,
	            Int64GetDatum(snapshot.snapshot_id));
	SysScanDesc scan = systable_beginscan(table, InvalidOid, false, NULL, 1, key);
	int count = 0;

	HeapTuple tuple;
	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		bool isnull;
		duckdb::DuckLakeSchemaInfo schema;
		int64_t end_snapshot = DatumGetInt64(heap_getattr(tuple, 4 /*end_snapshot*/, desc, &isnull));
		if (isnull || snapshot.snapshot_id < end_snapshot) {
			schema.id = duckdb::SchemaIndex(DatumGetInt64(heap_getattr(tuple, 1 /*schema_id*/, desc, &isnull)));
			Datum uuid = heap_getattr(tuple, 2 /*schema_uuid*/, desc, &isnull);
			schema.uuid = DatumGetCString(DirectFunctionCall1(uuid_out, uuid));
			schema.name = TextDatumGetCString(heap_getattr(tuple, 5 /*schema_name*/, desc, &isnull));
			Datum path = heap_getattr(tuple, 6 /*path*/, desc, &isnull);
			if (isnull) {
				schema.path = transaction.GetCatalog().DataPath();
			} else {
				duckdb::DuckLakePath path_info;
				path_info.path = TextDatumGetCString(path);
				path_info.path_is_relative = DatumGetBool(heap_getattr(tuple, 7 /*path_is_relative*/, desc, &isnull));
				D_ASSERT(!isnull);
				schema.path = FromRelativePath(path_info);
			}
			catalog_info.schemas.push_back(std::move(schema));
			count++;
		}
	}
	systable_endscan(scan);
	table_close(table, AccessShareLock);
	return count;
}

int
PgDuckLakeMetadataManager::GetDuckLakeTables(const duckdb::DuckLakeSnapshot &snapshot,
                                             duckdb::DuckLakeCatalogInfo &catalog) {
	::Relation table = table_open(DuckLakeTableOid(), AccessShareLock);
	TupleDesc desc = RelationGetDescr(table);
	ScanKeyData key[1];
	ScanKeyInit(&key[0], 3 /*begin_snapshot*/, BTLessEqualStrategyNumber, F_INT8LE,
	            Int64GetDatum(snapshot.snapshot_id));
	SysScanDesc scan = systable_beginscan(table, InvalidOid, false, NULL, 1, key);
	int count = 0;

	duckdb::map<duckdb::SchemaIndex, duckdb::idx_t> schema_map;
	for (idx_t i = 0; i < catalog.schemas.size(); i++) {
		schema_map[catalog.schemas[i].id] = i;
	}

	HeapTuple tuple;
	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		bool isnull;
		duckdb::DuckLakeTableInfo table_info;
		int64_t end_snapshot = DatumGetInt64(heap_getattr(tuple, 4 /*end_snapshot*/, desc, &isnull));
		if (isnull || snapshot.snapshot_id < end_snapshot) {
			table_info.id = duckdb::TableIndex(DatumGetInt64(heap_getattr(tuple, 1 /*table_id*/, desc, &isnull)));
			table_info.schema_id =
			    duckdb::SchemaIndex(DatumGetInt64(heap_getattr(tuple, 5 /*schema_id*/, desc, &isnull)));
			table_info.uuid =
			    DatumGetCString(DirectFunctionCall1(uuid_out, heap_getattr(tuple, 2 /*table_uuid*/, desc, &isnull)));
			table_info.name = TextDatumGetCString(heap_getattr(tuple, 6 /*table_name*/, desc, &isnull));

			auto schema_entry = schema_map.find(table_info.schema_id);
			D_ASSERT(schema_entry != schema_map.end());
			auto &schema = catalog.schemas[schema_entry->second];
			Datum path = heap_getattr(tuple, 7 /*path*/, desc, &isnull);
			if (isnull) {
				table_info.path = transaction.GetCatalog().DataPath();
			} else {
				duckdb::DuckLakePath path_info;
				path_info.path = TextDatumGetCString(path);
				path_info.path_is_relative = DatumGetBool(heap_getattr(tuple, 8 /*path_is_relative*/, desc, &isnull));
				D_ASSERT(!isnull);
				table_info.path = FromRelativePath(path_info, schema.path);
			}
			catalog.tables.push_back(std::move(table_info));
			count++;
		}
	}

	systable_endscan(scan);
	table_close(table, AccessShareLock);
	return count;
}

static int
GetDuckLakeTableDetails(const duckdb::DuckLakeSnapshot &snapshot, duckdb::DuckLakeTableInfo &table_info) {
	::Relation table = table_open(DuckLakeColumnOid(), AccessShareLock);
	TupleDesc desc = RelationGetDescr(table);
	ScanKeyData key[1];
	ScanKeyInit(&key[0], 4 /*table_id*/, BTGreaterEqualStrategyNumber, F_INT8EQ, Int64GetDatum(table_info.id.index));
	SysScanDesc scan = systable_beginscan(table, InvalidOid, false, NULL, 1, key);

	HeapTuple tuple;
	std::map<int64_t, duckdb::DuckLakeColumnInfo> column_map;
	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		bool isnull, isnulls[3];
		int64_t column_id = DatumGetInt64(heap_getattr(tuple, 1 /*column_id*/, desc, &isnulls[0]));
		int64_t begin_snapshot = DatumGetInt64(heap_getattr(tuple, 2 /*begin_snapshot*/, desc, &isnulls[1]));
		int64_t end_snapshot = DatumGetInt64(heap_getattr(tuple, 3 /*end_snapshot*/, desc, &isnulls[2]));
		if (isnulls[0] ||
		    (snapshot.snapshot_id >= begin_snapshot && (isnulls[2] || snapshot.snapshot_id < end_snapshot))) {
			duckdb::DuckLakeColumnInfo column_info;
			column_info.id = duckdb::FieldIndex(column_id);
			int64_t column_order = DatumGetInt64(heap_getattr(tuple, 5 /*column_order*/, desc, &isnull));
			column_info.name = TextDatumGetCString(heap_getattr(tuple, 6 /*column_name*/, desc, &isnull));
			column_info.type = TextDatumGetCString(heap_getattr(tuple, 7 /*column_type*/, desc, &isnull));
			Datum initial_default = heap_getattr(tuple, 8 /*initial_default*/, desc, &isnull);
			if (!isnull) {
				column_info.initial_default = duckdb::Value(TextDatumGetCString(initial_default));
			}
			Datum default_val = heap_getattr(tuple, 9 /*default_value*/, desc, &isnull);
			if (!isnull) {
				column_info.default_value = duckdb::Value(TextDatumGetCString(default_val));
			}
			column_info.nulls_allowed = DatumGetBool(heap_getattr(tuple, 10 /*nulls_allowed*/, desc, &isnull));
			// TODO: tags
			// TODO: parent_columns
			// table_info.columns.push_back(std::move(column_info));
			column_map[column_order] = std::move(column_info);
		}
	}

	for (auto &column : column_map) {
		table_info.columns.push_back(std::move(column.second));
	}

	systable_endscan(scan);
	table_close(table, AccessShareLock);
	return column_map.size();
}

bool
PgDuckLakeMetadataManager::GetDuckLakeTableInfo(const duckdb::DuckLakeSnapshot &snapshot,
                                                duckdb::DuckLakeSchemaEntry &schema_entry,
                                                duckdb::DuckLakeTableInfo &table_info) {
	::Relation table = table_open(DuckLakeTableOid(), AccessShareLock);
	TupleDesc desc = RelationGetDescr(table);
	ScanKeyData key[3];
	ScanKeyInit(&key[0], 3 /*begin_snapshot*/, BTLessEqualStrategyNumber, F_INT8LE,
	            Int64GetDatum(snapshot.snapshot_id));
	ScanKeyInit(&key[1], 5 /*schema_id*/, BTLessEqualStrategyNumber, F_INT8EQ,
	            Int64GetDatum(schema_entry.GetSchemaId().index));
	ScanKeyInit(&key[2], 6 /*table_name*/, BTLessEqualStrategyNumber, F_TEXTEQ,
	            CStringGetTextDatum(table_info.name.c_str()));
	SysScanDesc scan = systable_beginscan(table, InvalidOid, false, NULL, 3, key);
	int count = 0;

	HeapTuple tuple;
	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		bool isnull;
		int64_t end_snapshot = DatumGetInt64(heap_getattr(tuple, 4 /*end_snapshot*/, desc, &isnull));
		if (isnull || snapshot.snapshot_id < end_snapshot) {
			table_info.id = duckdb::TableIndex(DatumGetInt64(heap_getattr(tuple, 1 /*table_id*/, desc, &isnull)));
			table_info.schema_id =
			    duckdb::SchemaIndex(DatumGetInt64(heap_getattr(tuple, 5 /*schema_id*/, desc, &isnull)));
			table_info.uuid =
			    DatumGetCString(DirectFunctionCall1(uuid_out, heap_getattr(tuple, 2 /*table_uuid*/, desc, &isnull)));
			Datum path = heap_getattr(tuple, 7 /*path*/, desc, &isnull);
			if (isnull) {
				table_info.path = schema_entry.DataPath();
			} else {
				duckdb::DuckLakePath path_info;
				path_info.path = TextDatumGetCString(path);
				path_info.path_is_relative = DatumGetBool(heap_getattr(tuple, 8 /*path_is_relative*/, desc, &isnull));
				D_ASSERT(!isnull);
				table_info.path = FromRelativePath(path_info, schema_entry.DataPath());
			}
			++count;
		}
	}

	if (count > 1) {
		throw std::runtime_error("Multiple tables found for schema " +
		                         std::to_string(schema_entry.GetSchemaId().index) + " and table " + table_info.name);
	}

	if (count > 0) {
		GetDuckLakeTableDetails(snapshot, table_info);
	}

	systable_endscan(scan);
	table_close(table, AccessShareLock);
	return count > 0;
}

duckdb::DuckLakeCatalogInfo
PgDuckLakeMetadataManager::GetCatalogForSnapshot(duckdb::DuckLakeSnapshot ducklake_snapshot) {
	duckdb::DuckLakeCatalogInfo catalog_info;

	// --- 1. Schemas ---
	int nschemas = GetDuckLakeSchemas(ducklake_snapshot, catalog_info);
	pd_log(DEBUG2, "Read %d schemas", nschemas);

	// --- 2. Tables ---
	int ntables = GetDuckLakeTables(ducklake_snapshot, catalog_info);
	pd_log(DEBUG2, "Read %d tables", ntables);
	for (auto &table : catalog_info.tables) {
		int ncolumns = GetDuckLakeTableDetails(ducklake_snapshot, table);
		pd_log(DEBUG2, "Read %d columns for table %s", ncolumns, table.name.c_str());
	}

	// TODO: Views, Partitions (repeat similar pattern as above)

	return catalog_info;
}

duckdb::unique_ptr<duckdb::DuckLakeSnapshot>
PgDuckLakeMetadataManager::GetSnapshot() {
	::Relation table = table_open(DuckLakeSnapshotOid(), AccessShareLock);
	::Relation index = index_open(DuckLakeSnapshotIdOid(), AccessShareLock);
	TupleDesc desc = RelationGetDescr(table);

	ScanKeyData key[1];
	ScanKeyInit(&key[0], 1 /*snapshot_id*/, BTGreaterEqualStrategyNumber, F_INT8GE, Int64GetDatum(0));
	SysScanDesc scan = systable_beginscan_ordered(table, index, NULL, 1, key);

	HeapTuple tuple;
	bool isnulls[4];
	duckdb::unique_ptr<duckdb::DuckLakeSnapshot> ret = nullptr;

	// Get the first tuple (max snapshot_id due to BackwardScanDirection)
	if (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, BackwardScanDirection))) {
		auto snapshot_id = DatumGetInt64(heap_getattr(tuple, 1, desc, &isnulls[0]));
		auto schema_version = DatumGetInt64(heap_getattr(tuple, 3, desc, &isnulls[1]));
		auto next_catalog_id = DatumGetInt64(heap_getattr(tuple, 4, desc, &isnulls[2]));
		auto next_file_id = DatumGetInt64(heap_getattr(tuple, 5, desc, &isnulls[3]));

		D_ASSERT(!isnulls[0] && !isnulls[1] && !isnulls[2] && !isnulls[3]);
		ret = duckdb::make_uniq<duckdb::DuckLakeSnapshot>(snapshot_id, schema_version, next_catalog_id, next_file_id);
	}

	systable_endscan_ordered(scan);
	table_close(table, AccessShareLock);
	index_close(index, AccessShareLock);
	return ret;
}

duckdb::unique_ptr<duckdb::DuckLakeSnapshot>
PgDuckLakeMetadataManager::GetSnapshot(duckdb::BoundAtClause & /* at_clause */) {
	// TODO
	return nullptr;
}

duckdb::vector<duckdb::DuckLakeFileListEntry>
PgDuckLakeMetadataManager::GetFilesForTable(duckdb::DuckLakeTableEntry &table_entry,
                                            duckdb::DuckLakeSnapshot ducklake_snapshot,
                                            const duckdb::string & /*filter*/) {
	duckdb::vector<duckdb::DuckLakeFileListEntry> files;
	::Relation table = table_open(DuckLakeDataFileOid(), AccessShareLock);
	TupleDesc desc = RelationGetDescr(table);
	ScanKeyData key[2];
	ScanKeyInit(&key[0], 2 /*table_id*/, BTGreaterEqualStrategyNumber, F_INT8EQ,
	            Int64GetDatum(table_entry.GetTableId().index));
	ScanKeyInit(&key[1], 3 /*begin_snapshot*/, BTLessEqualStrategyNumber, F_INT8LE,
	            Int64GetDatum(ducklake_snapshot.snapshot_id));
	SysScanDesc scan = systable_beginscan(table, InvalidOid, false, NULL, 1, key);
	HeapTuple tuple;

	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		bool isnull;
		int64_t end_snapshot = DatumGetInt64(heap_getattr(tuple, 4 /*end_snapshot*/, desc, &isnull));
		if (isnull || ducklake_snapshot.snapshot_id < end_snapshot) {
			duckdb::DuckLakeFileListEntry file_entry;
			duckdb::DuckLakePath path;
			path.path = TextDatumGetCString(heap_getattr(tuple, 6 /*path*/, desc, &isnull));
			path.path_is_relative = DatumGetBool(heap_getattr(tuple, 7 /*path_is_relative*/, desc, &isnull));
			file_entry.file.path = FromRelativePath(path);
			file_entry.file.file_size_bytes = DatumGetInt64(heap_getattr(tuple, 10 /*file_size_bytes*/, desc, &isnull));
			file_entry.file.footer_size = DatumGetInt64(heap_getattr(tuple, 11 /*footer_size*/, desc, &isnull));
			file_entry.row_id_start = DatumGetInt64(heap_getattr(tuple, 12 /*row_id_start*/, desc, &isnull));
			file_entry.snapshot_id = DatumGetInt64(heap_getattr(tuple, 3 /*begin_snapshot*/, desc, &isnull));
			Datum partial_file_info = heap_getattr(tuple, 13 /*partial_file_info*/, desc, &isnull);
			if (!isnull) {
				(void)partial_file_info;
				// TODO handle partial file info
			}

			// TODO handle delete files
			files.push_back(std::move(file_entry));
		}
	}

	systable_endscan(scan);
	table_close(table, AccessShareLock);
	return files;
}

duckdb::vector<duckdb::DuckLakeGlobalStatsInfo>
PgDuckLakeMetadataManager::GetGlobalTableStats(duckdb::DuckLakeSnapshot /* snapshot */) {
	duckdb::vector<duckdb::DuckLakeGlobalStatsInfo> global_stats;

	// TODO: Implement

	return global_stats;
}

void
PgDuckLakeMetadataManager::WriteNewSchema(duckdb::DuckLakeSnapshot commit_snapshot,
                                          const duckdb::DuckLakeSchemaInfo &schema_info) {
	::Relation table = table_open(DuckLakeSchemaOid(), RowExclusiveLock);
	TupleDesc desc = RelationGetDescr(table);

	Datum values[7];
	bool nulls[7];
	memset(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum(schema_info.id.index);
	values[1] = DirectFunctionCall1(uuid_in, CStringGetDatum(schema_info.uuid.c_str()));
	values[2] = Int64GetDatum(commit_snapshot.snapshot_id);
	nulls[3] = true;
	values[4] = CStringGetTextDatum(schema_info.name.c_str());
	auto path = GetRelativePath(schema_info.path);
	values[5] = CStringGetTextDatum(path.path.c_str());
	values[6] = BoolGetDatum(path.path_is_relative);

	HeapTuple tuple = heap_form_tuple(desc, values, nulls);
	CatalogTupleInsert(table, tuple);
	table_close(table, NoLock);
}

void
PgDuckLakeMetadataManager::WriteNewSchemas(duckdb::DuckLakeSnapshot commit_snapshot,
                                           const duckdb::vector<duckdb::DuckLakeSchemaInfo> &new_schemas) {
	for (auto &schema : new_schemas) {
		WriteNewSchema(commit_snapshot, schema);
	}
}

void
PgDuckLakeMetadataManager::WriteNewTable(duckdb::DuckLakeSnapshot commit_snapshot,
                                         const duckdb::DuckLakeTableInfo &table_info) {
	::Relation table = table_open(DuckLakeTableOid(), RowExclusiveLock);
	TupleDesc desc = RelationGetDescr(table);

	Datum values[8];
	bool nulls[8];
	memset(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum(table_info.id.index);
	values[1] = DirectFunctionCall1(uuid_in, CStringGetDatum(table_info.uuid.c_str()));
	values[2] = Int64GetDatum(commit_snapshot.snapshot_id);
	nulls[3] = true;
	values[4] = Int64GetDatum(table_info.schema_id.index);
	values[5] = CStringGetTextDatum(table_info.name.c_str());
	auto path = GetRelativePath(table_info.schema_id, table_info.path);
	values[6] = CStringGetTextDatum(path.path.c_str());
	values[7] = BoolGetDatum(path.path_is_relative);

	HeapTuple tuple = heap_form_tuple(desc, values, nulls);
	CatalogTupleInsert(table, tuple);
	table_close(table, NoLock);

	// Write table columns
	::Relation col_table = table_open(DuckLakeColumnOid(), RowExclusiveLock);
	TupleDesc col_desc = RelationGetDescr(col_table);
	for (auto &column : table_info.columns) {
		Datum col_values[11];
		bool col_nulls[11];
		memset(col_nulls, 0, sizeof(col_nulls));

		col_values[0] = Int64GetDatum(column.id.index);             // column_id
		col_values[1] = Int64GetDatum(commit_snapshot.snapshot_id); // begin_snapshot
		col_nulls[2] = true;                                        // end_snapshot
		col_values[3] = Int64GetDatum(table_info.id.index);         // table_id
		col_values[4] = Int64GetDatum(column.id.index);             // column_order
		col_values[5] = CStringGetTextDatum(column.name.c_str());   // column_name
		col_values[6] = CStringGetTextDatum(column.type.c_str());   // column_type
		col_nulls[7] = true;                                        // initial_default
		col_nulls[8] = true;                                        // default_value
		col_values[9] = BoolGetDatum(column.nulls_allowed);         // nulls_allowed
		col_nulls[10] = true;                                       // parent_column

		HeapTuple col_tuple = heap_form_tuple(col_desc, col_values, col_nulls);
		CatalogTupleInsert(col_table, col_tuple);
	}

	table_close(col_table, NoLock);
}

void
PgDuckLakeMetadataManager::WriteNewTables(duckdb::DuckLakeSnapshot commit_snapshot,
                                          const duckdb::vector<duckdb::DuckLakeTableInfo> &new_tables) {
	for (auto &table : new_tables) {
		WriteNewTable(commit_snapshot, table);
	}
}

void
PgDuckLakeMetadataManager::InsertSnapshot(duckdb::DuckLakeSnapshot commit_snapshot) {
	::Relation table = table_open(DuckLakeSnapshotOid(), RowExclusiveLock);
	TupleDesc desc = RelationGetDescr(table);

	Datum values[5];
	bool nulls[5];
	memset(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum(commit_snapshot.snapshot_id);
	values[1] = TimestampTzGetDatum(GetCurrentTimestamp());
	values[2] = Int64GetDatum(commit_snapshot.schema_version);
	values[3] = Int64GetDatum(commit_snapshot.next_catalog_id);
	values[4] = Int64GetDatum(commit_snapshot.next_file_id);

	HeapTuple tuple = heap_form_tuple(desc, values, nulls);
	CatalogTupleInsert(table, tuple);
	table_close(table, NoLock);
}

void
PgDuckLakeMetadataManager::WriteSnapshotChanges(duckdb::DuckLakeSnapshot commit_snapshot,
                                                const duckdb::SnapshotChangeInfo &change_info) {
	::Relation table = table_open(DuckLakeSnapshotChangesOid(), RowExclusiveLock);
	TupleDesc desc = RelationGetDescr(table);

	Datum values[2];
	bool nulls[2];
	memset(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum(commit_snapshot.snapshot_id);
	if (change_info.changes_made.empty()) {
		nulls[1] = true;
	} else {
		values[1] = CStringGetTextDatum(change_info.changes_made.c_str());
	}

	HeapTuple tuple = heap_form_tuple(desc, values, nulls);
	CatalogTupleInsert(table, tuple);
	table_close(table, NoLock);
}

void
PgDuckLakeMetadataManager::WriteNewDataFiles(duckdb::DuckLakeSnapshot commit_snapshot,
                                             const duckdb::vector<duckdb::DuckLakeFileInfo> &new_files) {
	if (new_files.empty()) {
		return;
	}

	::Relation table = table_open(DuckLakeDataFileOid(), RowExclusiveLock);
	TupleDesc desc = RelationGetDescr(table);

	for (auto &file : new_files) {
		Datum values[15];
		bool nulls[15];
		memset(nulls, 0, sizeof(nulls));
		values[0] = Int64GetDatum(file.id.index);
		values[1] = Int64GetDatum(file.table_id.index);
		values[2] =
		    Int64GetDatum(file.begin_snapshot.IsValid() ? file.begin_snapshot.GetIndex() : commit_snapshot.snapshot_id);
		nulls[3] = true;
		nulls[4] = true;
		auto path = GetRelativePath(file.file_name);
		values[5] = CStringGetTextDatum(path.path.c_str());
		values[6] = BoolGetDatum(path.path_is_relative);
		values[7] = CStringGetTextDatum("parquet");
		values[8] = Int64GetDatum(file.row_count);
		values[9] = Int64GetDatum(file.file_size_bytes);
		if (file.footer_size.IsValid()) {
			values[10] = Int64GetDatum(file.footer_size.GetIndex());
		} else {
			nulls[10] = true;
		}
		if (file.row_id_start.IsValid()) {
			values[11] = Int64GetDatum(file.row_id_start.GetIndex());
		} else {
			nulls[11] = true;
		}
		if (file.partition_id.IsValid()) {
			values[12] = Int64GetDatum(file.partition_id.GetIndex());
		} else {
			nulls[12] = true;
		}
		if (file.encryption_key.empty()) {
			nulls[13] = true;
		} else {
			// TODO handle encryption key
		}
		if (file.partial_file_info.empty()) {
			nulls[14] = true;
		} else {
			// TODO handle partial file info
		}

		HeapTuple tuple = heap_form_tuple(desc, values, nulls);
		CatalogTupleInsert(table, tuple);
	}

	table_close(table, NoLock);
}

void
PgDuckLakeMetadataManager::UpdateGlobalTableStats(const duckdb::DuckLakeGlobalStatsInfo & /* stats */) {
	// TODO: Implement
}

} // namespace pgduckdb
