#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include <string>
#include <unordered_map>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_extension.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "catalog/objectaddress.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"

static bool is_background_worker = false;
static std::unordered_map<std::string, std::string> last_known_motherduck_catalog_versions;
static uint64 initial_cache_version = 0;

extern "C" {
PGDLLEXPORT void
pgduckdb_background_worker_main(Datum main_arg) {
	elog(LOG, "started pgduckdb background worker");
	// Set up a signal handler for SIGTERM
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(duckdb_background_worker_db, NULL, 0);

	pgduckdb::doing_motherduck_sync = true;
	is_background_worker = true;

	while (true) {
		// Initialize SPI (Server Programming Interface) and connect to the database
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (pgduckdb::IsExtensionRegistered()) {
			/*
			 * If the extension is not registerid this loop is a no-op, which
			 * means we essentially keep polling until the extension is
			 * installed
			 */
			pgduckdb::SyncMotherDuckCatalogsWithPg(false);
		}

		// Commit the transaction
		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();
		pgstat_report_stat(false);
		pgstat_report_activity(STATE_IDLE, NULL);

		// Wait for a second or until the latch is set
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1000L, PG_WAIT_EXTENSION);
		CHECK_FOR_INTERRUPTS();
		ResetLatch(MyLatch);
	}

	proc_exit(0);
}

PG_FUNCTION_INFO_V1(force_motherduck_sync);
Datum
force_motherduck_sync(PG_FUNCTION_ARGS) {
	Datum drop_with_cascade = PG_GETARG_BOOL(0);
	/* clear the cache of known catalog versions to force a full sync */
	last_known_motherduck_catalog_versions.clear();
	SPI_connect_ext(SPI_OPT_NONATOMIC);
	PG_TRY();
	{
		pgduckdb::doing_motherduck_sync = true;
		pgduckdb::SyncMotherDuckCatalogsWithPg(drop_with_cascade);
	}
	PG_FINALLY();
	{ pgduckdb::doing_motherduck_sync = false; }
	PG_END_TRY();
	SPI_finish();
	PG_RETURN_VOID();
}
}

void
DuckdbInitBackgroundWorker(void) {
	if (!pgduckdb::IsMotherDuckEnabled()) {
		return;
	}

	BackgroundWorker worker;
	// Set up the worker struct
	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_duckdb");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "pgduckdb_background_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_duckdb sync worker");
	worker.bgw_restart_time = 1;
	worker.bgw_main_arg = (Datum)0;

	// Register the worker
	RegisterBackgroundWorker(&worker);
}

namespace pgduckdb {
bool doing_motherduck_sync;

static std::string
PgSchemaName(std::string db_name, std::string schema_name, bool is_default_db) {
	if (is_default_db) {
		return schema_name;
	}
	std::string escaped_db_name = duckdb::KeywordHelper::EscapeQuotes(db_name, '$');
	if (schema_name == "main") {
		return "ddb$" + escaped_db_name;
	}
	return "ddb$" + escaped_db_name + "$" + duckdb::KeywordHelper::EscapeQuotes(schema_name, '$');
}

static std::string
DropPgTableString(const char *postgres_schema_name, const char *table_name, bool with_cascade) {
	std::string ret = "";
	ret += "DROP TABLE ";
	ret += duckdb::KeywordHelper::WriteQuoted(postgres_schema_name, '"');
	ret += ".";
	ret += duckdb::KeywordHelper::WriteQuoted(table_name, '"');
	ret += with_cascade ? " CASCADE; " : "; ";
	return ret;
}

static std::string
CreatePgTableString(duckdb::CreateTableInfo &info, bool is_default_db) {
	std::string ret = "";

	ret += "CREATE TABLE ";
	std::string schema_name = PgSchemaName(info.catalog, info.schema, is_default_db);
	ret += duckdb::KeywordHelper::WriteQuoted(schema_name, '"');
	ret += ".";
	ret += duckdb::KeywordHelper::WriteQuoted(info.table, '"');
	ret += "(";

	bool first = true;
	for (auto &column : info.columns.Logical()) {
		if (!first) {
			ret += ", ";
		}
		ret += duckdb::KeywordHelper::WriteQuoted(column.Name(), '"');
		ret += " ";
		Oid postgres_type = GetPostgresDuckDBType(column.Type());
		if (postgres_type == InvalidOid) {
			elog(WARNING, "Skipping table %s.%s.%s due to unsupported type", info.catalog.c_str(), info.schema.c_str(),
			     info.table.c_str());

			return "";
		}
		int32 typemod = GetPostgresDuckDBTypemod(column.Type());
		ret += format_type_with_typemod(postgres_type, typemod);
		first = false;
	}
	ret += ") USING duckdb;";
	return ret;
}

static std::string
CreatePgSchemaString(std::string postgres_schema_name) {
	return "CREATE SCHEMA " + duckdb::KeywordHelper::WriteQuoted(postgres_schema_name, '"') + ";";
}

/*
 * This function is a workaround for the fact that SPI_commit() does not work
 * well in background workers. You get weird errors like:
 * ERROR: portal snapshots (0) did not account for all active snapshots (1)
 *
 * Luckily we don't really care, all we really care about is that we quickly
 * release the heavy locks we hold on tables after dropping/creating them. And
 * this can easily be done by simply finishing the transaction and opening a
 * new one.
 *
 * This workaround is only necessary in background workers, in normal sessions
 * where people call force_motherduck_sync wo still want to use SPI_commit().
 *
 * See the following thread for details:
 * https://www.postgresql.org/message-id/flat/CAFcNs%2Bp%2BfD5HEXEiZMZC1COnXkJCMnUK0%3Dr4agmZP%3D9Hi%2BYcJA%40mail.gmail.com
 */
void
SPI_commit_that_works_in_bgworker() {
	if (is_background_worker) {
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
	} else {
		SPI_commit();
	}
	if (initial_cache_version != pgduckdb::CacheVersion()) {
		if (is_background_worker) {
			elog(ERROR, "DuckDB cache version changed during sync, aborting sync, background worker will restart "
			            "automatically");
		} else {
			elog(ERROR, "DuckDB cache version changed during sync, aborting sync, please try again");
		}
	}
}

static bool
CreateTable(const char *postgres_schema_name, const char *table_name, std::string create_table_query,
            bool drop_with_cascade) {
	/*
	 * We need to fetch this over-and-over again, because we commit the
	 * transaction and thus release locks. So in theory the schema could be
	 * deleted/renamed etc.
	 */
	Oid schema_oid = get_namespace_oid(postgres_schema_name, false);
	HeapTuple tuple = SearchSysCache2(RELNAMENSP, CStringGetDatum(table_name), ObjectIdGetDatum(schema_oid));
	if (HeapTupleIsValid(tuple)) {
		Form_pg_class postgres_relation = (Form_pg_class)GETSTRUCT(tuple);
		/* The table already exists in Postgres, so we cannot simply create it. */

		if (!IsMotherDuckTable(postgres_relation)) {
			/*
			 * Oops, we have a conflict. Let's notify the user, and
			 * not do anything else
			 */
			elog(WARNING,
			     "Skipping sync of MotherDuck table %s.%s because its name conflicts with an "
			     "already existing table/view/index in Postgres",
			     postgres_schema_name, table_name);
			ReleaseSysCache(tuple);
			return false;
		}
		ReleaseSysCache(tuple);

		/*
		 * It's an old version of this DuckDB table, we can safely
		 * drop it and recreate it.
		 */
		auto drop_query = DropPgTableString(postgres_schema_name, table_name, drop_with_cascade);
		create_table_query = drop_query + create_table_query;
	}

	MemoryContext old_context = MemoryContextSwitchTo(CurrentMemoryContext);
	int ret;

	/*
	 * We create a subtransaction to be able to cleanly roll back in case of
	 * any errors.
	 */
	BeginInternalSubTransaction(NULL);
	PG_TRY();
	{ ret = SPI_exec(create_table_query.c_str(), 0); }
	PG_CATCH();
	{
		MemoryContextSwitchTo(old_context);
		ErrorData *edata = CopyErrorData();
		ereport(WARNING, (errmsg("Failed to sync MotherDuck table %s.%s", postgres_schema_name, table_name),

		                  errdetail("While executing command: %s", create_table_query.c_str()),
		                  errhint("See next WARNING for details")));
		edata->elevel = WARNING;
		ThrowErrorData(edata);
		FreeErrorData(edata);
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		return false;
	}
	PG_END_TRY();

	if (ret != SPI_OK_UTILITY) {
		elog(WARNING, "SPI_execute failed: error code %d", ret);
		RollbackAndReleaseCurrentSubTransaction();
		return false;
	}
	/* Success, so we commit the subtransaction */
	ReleaseCurrentSubTransaction();
	/* And then we also commit the actual transaction to release any locks that
	 * were necessary to execute it. */
	SPI_commit_that_works_in_bgworker();
	return true;
}

static bool
CreateSchemaIfNotExists(const char *postgres_schema_name, bool is_default_db) {
	Oid schema_oid = get_namespace_oid(postgres_schema_name, true);
	if (schema_oid != InvalidOid) {
		return true;
	}

	std::string create_schema_query = CreatePgSchemaString(postgres_schema_name);

	MemoryContext old_context = MemoryContextSwitchTo(CurrentMemoryContext);
	int ret;

	/*
	 * We create a subtransaction to be able to cleanly roll back in case of
	 * any errors.
	 */
	BeginInternalSubTransaction(NULL);
	PG_TRY();
	{ ret = SPI_exec(create_schema_query.c_str(), 0); }
	PG_CATCH();
	{
		MemoryContextSwitchTo(old_context);
		ErrorData *edata = CopyErrorData();
		ereport(WARNING, (errmsg("Failed to sync MotherDuck schema %s", postgres_schema_name),
		                  errdetail("While executing command: %s", create_schema_query.c_str()),
		                  errhint("See next WARNING for details")));
		edata->elevel = WARNING;
		ThrowErrorData(edata);
		FreeErrorData(edata);
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		return false;
	}
	PG_END_TRY();

	if (ret != SPI_OK_UTILITY) {
		elog(WARNING, "SPI_execute failed: error code %d", ret);
		RollbackAndReleaseCurrentSubTransaction();
		return false;
	}
	/* Success, so we commit the subtransaction */
	ReleaseCurrentSubTransaction();

	if (!is_default_db) {
		/*
		 * For ddb$ schemas we need to record a dependency between the schema
		 * and the extension, so that DROP EXTENSION also drops these schemas.
		 */
		schema_oid = get_namespace_oid(postgres_schema_name, false);
		ObjectAddress schema_address = {
		    .classId = NamespaceRelationId,
		    .objectId = schema_oid,
		};
		ObjectAddress extension_address = {
		    .classId = ExtensionRelationId,
		    .objectId = pgduckdb::ExtensionOid(),
		};
		recordDependencyOn(&schema_address, &extension_address, DEPENDENCY_NORMAL);
	}

	/* And then we also commit the actual transaction to release any locks that
	 * were necessary to execute it. */
	SPI_commit_that_works_in_bgworker();
	return true;
}

void
SyncMotherDuckCatalogsWithPg(bool drop_with_cascade) {
	initial_cache_version = pgduckdb::CacheVersion();

	auto connection = pgduckdb::DuckDBManager::Get().CreateConnection();
	auto &context = *connection->context;
	if (!pgduckdb::IsMotherDuckEnabled()) {
		elog(ERROR, "MotherDuck support is not enabled");
	}

	auto &db_manager = duckdb::DatabaseManager::Get(context);
	auto default_db = db_manager.GetDefaultDatabase(context);
	auto result =
	    context.Query("SELECT alias, server_catalog_version::text FROM __md_local_databases_metadata()", false);
	if (result->HasError()) {
		elog(WARNING, "Failed to fetch MotherDuck database metadata: %s", result->GetError().c_str());
		return;
	}
	context.transaction.BeginTransaction();
	for (auto &row : *result) {
		auto motherduck_db = row.GetValue<std::string>(0);
		auto catalog_version = row.GetValue<std::string>(1);
		if (last_known_motherduck_catalog_versions[motherduck_db] == catalog_version) {
			/* The catalog version has not changed, we can skip this database */
			continue;
		}
		/* The catalog version has changed, we need to sync the catalog */
		last_known_motherduck_catalog_versions[motherduck_db] = catalog_version;
		auto schemas = duckdb::Catalog::GetSchemas(context, motherduck_db);
		bool is_default_db = motherduck_db == default_db;
		for (duckdb::SchemaCatalogEntry &schema : schemas) {
			if (schema.name == "information_schema" || schema.name == "pg_catalog") {
				continue;
			}

			std::string postgres_schema_name = PgSchemaName(motherduck_db, schema.name, is_default_db);

			CreateSchemaIfNotExists(postgres_schema_name.c_str(), is_default_db);

			schema.Scan(context, duckdb::CatalogType::TABLE_ENTRY, [&](duckdb::CatalogEntry &entry) {
				if (entry.type != duckdb::CatalogType::TABLE_ENTRY) {
					return;
				}
				auto &table = entry.Cast<duckdb::TableCatalogEntry>();
				auto storage_info = table.GetStorageInfo(context);

				auto table_info = duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTableInfo>(table.GetInfo());
				table_info->schema = table.schema.name;
				table_info->catalog = motherduck_db;

				auto create_query = CreatePgTableString(*table_info, is_default_db);
				if (create_query.empty()) {
					return;
				}

				if (!CreateTable(postgres_schema_name.c_str(), table.name.c_str(), create_query.c_str(),
				                 drop_with_cascade)) {
					return;
				}
			});
		}
	}
	context.transaction.Commit();
}
} // namespace pgduckdb
