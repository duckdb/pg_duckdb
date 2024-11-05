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
#include "catalog/pg_authid.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_extension.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "catalog/objectaddress.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_guc.h"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

static bool is_background_worker = false;
static std::unordered_map<std::string, std::string> last_known_motherduck_catalog_versions;
static uint64 initial_cache_version = 0;

extern "C" {
PGDLLEXPORT void
pgduckdb_background_worker_main(Datum main_arg) {
	elog(LOG, "started pg_duckdb background worker");
	// Set up a signal handler for SIGTERM
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(duckdb_motherduck_postgres_database, NULL, 0);

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
	if (!pgduckdb::IsMotherDuckEnabledAnywhere()) {
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
/* Global variables that are used to communicate with our event triggers so
 * they handle DDL of syncing differently than user-initiated DDL */
bool doing_motherduck_sync;
char *current_motherduck_catalog_version;

static std::string
PgSchemaName(const std::string &db_name, const std::string &schema_name, bool is_default_db) {
	if (is_default_db) {
		/*
		 * We map the "main" DuckDB schema of the default database to the
		 * "public" Postgres schema, because they are pretty much equivalent
		 * from a user perspective even though they are named differently.
		 */
		if (schema_name == "main") {
			return "public";
		}
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
		Oid postgres_type = GetPostgresDuckDBType(column.Type());
		if (postgres_type == InvalidOid) {
			elog(WARNING, "Skipping column %s in table %s.%s.%s due to unsupported type", column.Name().c_str(),
			     info.catalog.c_str(), info.schema.c_str(), info.table.c_str());
			continue;
		}

		if (!first) {
			ret += ", ";
		}
		first = false;

		ret += duckdb::KeywordHelper::WriteQuoted(column.Name(), '"');
		ret += " ";
		int32 typemod = GetPostgresDuckDBTypemod(column.Type());
		ret += format_type_with_typemod(postgres_type, typemod);
	}
	if (first) {
		elog(WARNING, "Skipping table %s.%s.%s because non of its columns had supported types", info.catalog.c_str(),
		     info.schema.c_str(), info.table.c_str());
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

/*
 * This function runs a utility command in the current SPI context. Instead of
 * throwing an ERROR on failure this will throw a WARNING and return false. It
 * does so in a way that preserves the current transaction state, so that other
 * commands can still be run.
 */
static bool
SPI_run_utility_command(const char *query) {
	MemoryContext old_context = CurrentMemoryContext;
	int ret;
	/*
	 * We create a subtransaction to be able to cleanly roll back in case of
	 * any errors.
	 */
	BeginInternalSubTransaction(NULL);
	PG_TRY();
	{ ret = SPI_exec(query, 0); }
	PG_CATCH();
	{
		MemoryContextSwitchTo(old_context);
		ErrorData *edata = CopyErrorData();
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
	return true;
}

static bool
CreateTable(const char *postgres_schema_name, const char *table_name, const char *create_table_query,
            bool drop_with_cascade) {
	/*
	 * We need to fetch this over-and-over again, because we commit the
	 * transaction and thus release locks. So in theory the schema could be
	 * deleted/renamed etc.
	 */
	Oid schema_oid = get_namespace_oid(postgres_schema_name, false);
	HeapTuple tuple = SearchSysCache2(RELNAMENSP, CStringGetDatum(table_name), ObjectIdGetDatum(schema_oid));

	bool did_delete_table = false;
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
		std::string drop_table_query = DropPgTableString(postgres_schema_name, table_name, drop_with_cascade);

		/* We use this to roll back the drop if the CREATE after fails */
		BeginInternalSubTransaction(NULL);

		/* Revert back to original privileges */
		if (!SPI_run_utility_command(drop_table_query.c_str())) {

			ereport(WARNING, (errmsg("Failed to sync MotherDuck table %s.%s", postgres_schema_name, table_name),

			                  errdetail("While executing command: %s", create_table_query),
			                  errhint("See previous WARNING for details")));
			/*
			 * Rollback the subtransaction to clean up the subtransaction
			 * state. Even though there's nothing actually in it. So we could
			 * we could just as well commit it, but rolling back seems more
			 * sensible.
			 */
			RollbackAndReleaseCurrentSubTransaction();
			return false;
		}

		did_delete_table = true;
	}

	Oid saved_userid;
	int sec_context;
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(MotherDuckPostgresUser(), sec_context | SECURITY_LOCAL_USERID_CHANGE);
	bool create_table_succeeded = SPI_run_utility_command(create_table_query);
	SetUserIdAndSecContext(saved_userid, sec_context);
	/* Revert back to original privileges */
	if (!create_table_succeeded) {

		ereport(WARNING, (errmsg("Failed to sync MotherDuck table %s.%s", postgres_schema_name, table_name),

		                  errdetail("While executing command: %s", create_table_query),
		                  errhint("See previous WARNING for details")));
		if (did_delete_table) {
			/* Rollback the drop that succeeded */
			RollbackAndReleaseCurrentSubTransaction();
		}
		return false;
	}

	if (did_delete_table) {
		/*
		 * Commit the subtransaction that contains both the drop and the create that
		 * contains the actual table creation.
		 */
		ReleaseCurrentSubTransaction();
	}

	/* And then we also commit the actual transaction to release any locks that
	 * were necessary to execute it. */
	SPI_commit_that_works_in_bgworker();
	return true;
}

static bool
DropTable(const char *fully_qualified_table, bool drop_with_cascade) {
	const char *query = psprintf("DROP TABLE %s%s", fully_qualified_table, drop_with_cascade ? " CASCADE" : "");

	if (!SPI_run_utility_command(query)) {
		ereport(WARNING,
		        (errmsg("Failed to drop deleted MotherDuck table %s", fully_qualified_table),

		         errdetail("While executing command: %s", query), errhint("See previous WARNING for details")));
		return false;
	}
	/*
	 * We explicitely don't call SPI_commit_that_works_in_background_worker
	 * here, because that makes transactional considerations easier. And when
	 * deleting tables, it doesn't matter how long we keep locks on them,
	 * because they are already deleted upstream so there can be no queries on
	 * them anyway.
	 */
	return true;
}

/*
 * We should grant access on schemas that we want to create tables in.
 *
 * Returns if access was granted successfully, in some cases we don't do this
 * for security reasons.
 */
static bool
GrantAccessToSchema(const char *postgres_schema_name) {
	if (pgduckdb::MotherDuckPostgresUser() == BOOTSTRAP_SUPERUSERID) {
		/*
		 * We don't need to grant access to the bootstrap superuser. It already
		 * has every access it might need.
		 */
		return true;
	}

	/* Grant access to the schema to the current user */
	const char *grant_query =
	    psprintf("GRANT ALL ON SCHEMA %s TO %s", postgres_schema_name, quote_identifier(duckdb_postgres_role));
	if (!SPI_run_utility_command(grant_query)) {
		ereport(WARNING,
		        (errmsg("Failed to grant access to MotherDuck schema %s", postgres_schema_name),
		         errdetail("While executing command: %s", grant_query), errhint("See previous WARNING for details")));
		return false;
	}
	return true;
}

static bool
CreateSchemaIfNotExists(const char *postgres_schema_name, bool is_default_db) {
	Oid schema_oid = get_namespace_oid(postgres_schema_name, true);
	if (schema_oid != InvalidOid) {
		/*
		 * Let's check if the duckdb.postgres_user can actually create tables
		 * in this schema. Surprisingly the USAGE permission is not needed to
		 * create tables in a schema, only to list the tables. So we dont' need
		 * to check for that one.
		 */

#if PG_VERSION_NUM >= 160000
		bool user_has_create_access =
		    object_aclcheck(NamespaceRelationId, schema_oid, MotherDuckPostgresUser(), ACL_CREATE) == ACLCHECK_OK;
#else
		bool user_has_create_access =
		    pg_namespace_aclcheck(schema_oid, MotherDuckPostgresUser(), ACL_CREATE) == ACLCHECK_OK;
#endif
		if (user_has_create_access) {
			return true;
		}
		if (is_default_db) {
			/*
			 * For non $ddb schemas that already exist we don't want to give
			 * CREATE privileges to the duckdb.posgres_role automatically. It
			 * might be some restricted and an attacker with MotherDuck access
			 * should not be able to create tables in it unless the DBA has
			 * configured access this way.
			 */
			ereport(WARNING, (errmsg("MotherDuck schema %s already exists, but duckdb.postgres_user does not have "
			                         "CREATE privileges on it",
			                         postgres_schema_name),
			                  errhint("You might want to grant ALL privileges to the user '%s' on this schema.",
			                          duckdb_postgres_role)));
			return false;
		}

		/*
		 * We always want to grant access for ddb$ schemas. They are
		 * effictively owned by the extension so MotherDuck users should be
		 * able to do with them whatever they want. GrantAccessToSchema will
		 * already log a WARNING if it fails.
		 */
		return GrantAccessToSchema(postgres_schema_name);
	}

	std::string create_schema_query = CreatePgSchemaString(postgres_schema_name);

	/* We want to group the CREATE SCHEMA and the GRANT in a subtransaction */
	BeginInternalSubTransaction(NULL);

	if (!SPI_run_utility_command(create_schema_query.c_str())) {
		ereport(WARNING, (errmsg("Failed to sync MotherDuck schema %s", postgres_schema_name),
		                  errdetail("While executing command: %s", create_schema_query.c_str()),
		                  errhint("See previous WARNING for details")));
		RollbackAndReleaseCurrentSubTransaction();
		return false;
	}

	/*
	 * And let's GRANT access to the schema. Even for non-$ddb schemas this
	 * should be safe, because it's a new schema so no-one depends upon it not
	 * having unexpected tables in it. So basically, because it didn't exist
	 * yet we now claim it.
	 */
	if (!GrantAccessToSchema(postgres_schema_name)) {
		/* GrantAccessToSchema already logged a WARNING */
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

void SyncMotherDuckCatalogsWithPg_Cpp(bool drop_with_cascade);

void
SyncMotherDuckCatalogsWithPg(bool drop_with_cascade) {
	InvokeCPPFunc(SyncMotherDuckCatalogsWithPg_Cpp, drop_with_cascade);
}

void
SyncMotherDuckCatalogsWithPg_Cpp(bool drop_with_cascade) {
	if (!pgduckdb::IsMotherDuckEnabled()) {
		throw std::runtime_error("MotherDuck support is not enabled");
	}

	initial_cache_version = pgduckdb::CacheVersion();

	auto connection = pgduckdb::DuckDBManager::Get().CreateConnection();
	auto &context = *connection->context;

	auto &db_manager = duckdb::DatabaseManager::Get(context);
	const auto& default_db = db_manager.GetDefaultDatabase(context);
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
		elog(LOG, "Syncing MotherDuck catalog for database %s: %s", motherduck_db.c_str(), catalog_version.c_str());

		/*
		 * Because of our SPI_commit_that_works_in_bgworker() workaround we need to
		 * switch to TopMemoryContext before allocating any memory that
		 * should survive a call to SPI_commit_that_works_in_bgworker(). To
		 * avoid leaking unboundedly when errors occur we free the memory
		 * that's already there if not already NULL.
		 */
		MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
		if (current_motherduck_catalog_version) {
			pfree(current_motherduck_catalog_version);
		}
		current_motherduck_catalog_version = pstrdup(catalog_version.c_str());
		MemoryContextSwitchTo(old_context);

		last_known_motherduck_catalog_versions[motherduck_db] = catalog_version;
		auto schemas = duckdb::Catalog::GetSchemas(context, motherduck_db);
		bool is_default_db = motherduck_db == default_db;
		bool all_tables_synced_successfully = true;
		for (duckdb::SchemaCatalogEntry &schema : schemas) {
			if (schema.name == "information_schema" || schema.name == "pg_catalog") {
				continue;
			}

			if (schema.name == "public" && is_default_db) {
				/*
				 * We already map the "main" duckdb schema to the "public"
				 * Postgres. This could cause conflicts and confusion if we
				 * also map the "public" duckdb schema to the "public"
				 * Postgres schema. Since there is unlikely to be a "public"
				 * schema in the DuckDB database, we simply skip this schema
				 * for now. If users actually turn out to run into this, we
				 * probably want to map it to something else for example to
				 * ddb$my_db$public. Users can also always rename the "public"
				 * schema in their DuckDB database to something else.
				 */
				elog(
				    WARNING,
				    "Skipping sync of MotherDuck schema %s.%s because it conflicts with the sync of the %s.main schema",
				    motherduck_db.c_str(), schema.name.c_str(), motherduck_db.c_str());
				continue;
			}

			std::string postgres_schema_name = PgSchemaName(motherduck_db, schema.name, is_default_db);

			if (!CreateSchemaIfNotExists(postgres_schema_name.c_str(), is_default_db)) {
				/* We failed to create the schema, so we skip the tables in it */
				continue;
			}

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
					all_tables_synced_successfully = false;
					return;
				}
			});
		}

		/*
		 * Now we want drop all shell tables in this database that were not
		 * updated by the current round. Since if they were not updated that
		 * means they are not part of the MotherDuck database anymore,
		 * except...
		 */
		if (!all_tables_synced_successfully) {
			/*
			 * ...if not all tables in this catalog were synced successfully,
			 * then the user should fix that problem before we dare to clean
			 * up. Otherwise we might accidentally cleanup a table that is
			 * still there, but which failed to be updated for some reason.
			 *
			 * We rely on the database operator to fix the syncing problem, and
			 * after that's done we'll clean up the tables during that first
			 * working sync. We could probably do something smarter here, but
			 * for now this is okay.
			 */
			continue;
		}

		Oid arg_types[] = {TEXTOID, TEXTOID};
		Datum values[] = {CStringGetTextDatum(motherduck_db.c_str()),
		                  CStringGetTextDatum(pgduckdb::current_motherduck_catalog_version)};

		/*
		 * We use a cursor here instead of plain SPI_execute, to put a limit on
		 * the memory usage here in case many tables need to be deleted (e.g.
		 * some big schema was deleted all at once). This is probably not
		 * necessary in most cases.
		 */
		Portal deleted_tables_portal =
		    SPI_cursor_open_with_args(nullptr, R"(
			SELECT relid::text FROM duckdb.tables
			WHERE duckdb_db = $1 AND motherduck_catalog_version != $2
			)",
		                              lengthof(arg_types), arg_types, values, NULL, false, 0);

		const int deleted_tables_batch_size = 100;
		int current_batch_size;
		do {
			SPI_cursor_fetch(deleted_tables_portal, true, deleted_tables_batch_size);
			SPITupleTable *deleted_tables_batch = SPI_tuptable;
			current_batch_size = SPI_processed;
			for (auto i = 0; i < current_batch_size; i++) {
				HeapTuple tuple = deleted_tables_batch->vals[i];
				char *fully_qualified_table = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
				/*
				 * We need to create a new SPI context for the DROP command
				 * otherwise our batch gets invalidated.
				 */
				SPI_connect();
				DropTable(fully_qualified_table, drop_with_cascade);
				SPI_finish();
			}
		} while (current_batch_size == deleted_tables_batch_size);
		SPI_cursor_close(deleted_tables_portal);

		SPI_commit_that_works_in_bgworker();

		pfree(current_motherduck_catalog_version);
		current_motherduck_catalog_version = nullptr;
	}
	context.transaction.Commit();
}
} // namespace pgduckdb
