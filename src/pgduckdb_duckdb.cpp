#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "pgduckdb/pgduckdb_guc.h"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/scan/postgres_view.hpp"
#include "pgduckdb/pg/transactions.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/namespace.h"
#include "lib/stringinfo.h"
#include "utils/lsyscache.h"  // get_relname_relid
#include "utils/fmgrprotos.h" // pg_sequence_last_value
#include "common/file_perm.h"
#include "miscadmin.h" // superuser
}

#include "pgduckdb/pgduckdb_options.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"

#include <sys/stat.h>
#include <unistd.h>

// FIXME - this file should not depend on PG, rather DataDir should be provided
extern char *DataDir;

namespace pgduckdb {

static bool
CheckDirectory(const std::string &directory) {
	struct stat info;

	if (lstat(directory.c_str(), &info) != 0) {
		if (errno == ENOENT) {
			elog(DEBUG2, "Directory `%s` doesn't exists.", directory.c_str());
			return false;
		} else if (errno == EACCES) {
			throw std::runtime_error("Can't access `" + directory + "` directory.");
		} else {
			throw std::runtime_error("Other error when reading `" + directory + "`.");
		}
	}

	if (!S_ISDIR(info.st_mode)) {
		elog(WARNING, "`%s` is not directory.", directory.c_str());
	}

	if (access(directory.c_str(), R_OK | W_OK)) {
		throw std::runtime_error("Directory `" + std::string(directory) + "` permission problem.");
	}

	return true;
}

std::string
CreateOrGetDirectoryPath(const char *directory_name) {
	std::ostringstream oss;
	oss << DataDir << "/" << directory_name;
	const auto duckdb_data_directory = oss.str();

	if (!CheckDirectory(duckdb_data_directory)) {
		if (mkdir(duckdb_data_directory.c_str(), pg_dir_create_mode) == -1) {
			throw std::runtime_error("Creating data directory '" + duckdb_data_directory + "' failed: `" +
			                         strerror(errno) + "`");
		}

		elog(DEBUG2, "Created %s directory", duckdb_data_directory.c_str());
	};

	return duckdb_data_directory;
}

static char *
uri_escape(const char *str) {
	StringInfoData buf;
	initStringInfo(&buf);

	while (*str) {
		char c = *str++;
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' ||
		    c == '.' || c == '~') {
			appendStringInfoChar(&buf, c);
		} else {
			appendStringInfo(&buf, "%%%02X", (unsigned char)c);
		}
	}

	return buf.data;
}

namespace ddb {
bool
DidWrites() {
	if (!DuckDBManager::IsInitialized()) {
		return false;
	}
	auto connection = DuckDBManager::GetConnectionUnsafe();
	auto &context = *connection->context;
	return DidWrites(context);
}

bool
DidWrites(duckdb::ClientContext &context) {
	if (!context.transaction.HasActiveTransaction()) {
		return false;
	}
	return context.ActiveTransaction().ModifiedDatabase() != nullptr;
}
} // namespace ddb

DuckDBManager DuckDBManager::manager_instance;

DuckDBManager::DuckDBManager() {
}

#define SET_DUCKDB_OPTION(ddb_option_name)                                                                             \
	config.options.ddb_option_name = duckdb_##ddb_option_name;                                                         \
	elog(DEBUG2,                                                                                                       \
	     "[PGDuckDB] Set DuckDB option: '" #ddb_option_name "'="                                                       \
	     "%s",                                                                                                         \
	     std::to_string(duckdb_##ddb_option_name).c_str());

void
DuckDBManager::Initialize() {
	elog(DEBUG2, "(PGDuckDB/DuckDBManager) Creating DuckDB instance");

	duckdb::DBConfig config;
	config.SetOptionByName("custom_user_agent", "pg_duckdb");
	config.SetOptionByName("extension_directory", CreateOrGetDirectoryPath("duckdb_extensions"));
	// Transforms VIEWs into their view definition
	config.replacement_scans.emplace_back(pgduckdb::PostgresViewScan);
	SET_DUCKDB_OPTION(allow_unsigned_extensions);
	SET_DUCKDB_OPTION(enable_external_access);
	SET_DUCKDB_OPTION(autoinstall_known_extensions);
	SET_DUCKDB_OPTION(autoload_known_extensions);

	if (duckdb_maximum_memory != NULL) {
		config.options.maximum_memory = duckdb::DBConfig::ParseMemoryLimit(duckdb_maximum_memory);
		elog(DEBUG2, "[PGDuckDB] Set DuckDB option: 'maximum_memory'=%s", duckdb_maximum_memory);
	}

	if (duckdb_maximum_threads > -1) {
		SET_DUCKDB_OPTION(maximum_threads);
	}

	const char *connection_string = nullptr;

	/*
	 * If MotherDuck is enabled, use it to connect to DuckDB. That way DuckDB
	 * its default database will be set to the default MotherDuck database.
	 */
	if (pgduckdb::IsMotherDuckEnabled()) {
		/*
		 * Disable web login for MotherDuck. Having support for web login could
		 * be nice for demos, but because the received token is not shared
		 * between backends you will get browser windows popped up. Sharing the
		 * token across backends could be made to work but the implementing it
		 * is not trivial, so for now we simply disable the web login.
		 */
		setenv("motherduck_disable_web_login", "1", 1);
		duckdb_motherduck_default_database = uri_escape(duckdb_motherduck_default_database);
		if (duckdb_motherduck_token[0] == '\0') {
			connection_string = psprintf("md:%s", duckdb_motherduck_default_database);
		} else {
			connection_string =
			    psprintf("md:%s?motherduck_token=%s", duckdb_motherduck_default_database, duckdb_motherduck_token);
		}
	}

	database = new duckdb::DuckDB(connection_string, &config);

	auto &dbconfig = duckdb::DBConfig::GetConfig(*database->instance);
	dbconfig.storage_extensions["pgduckdb"] = duckdb::make_uniq<PostgresStorageExtension>();
	duckdb::ExtensionInstallInfo extension_install_info;
	database->instance->SetExtensionLoaded("pgduckdb", extension_install_info);

	connection = duckdb::make_uniq<duckdb::Connection>(*database);

	auto &context = *connection->context;

	auto &db_manager = duckdb::DatabaseManager::Get(context);
	default_dbname = db_manager.GetDefaultDatabase(context);
	pgduckdb::DuckDBQueryOrThrow(context, "ATTACH DATABASE 'pgduckdb' (TYPE pgduckdb)");
	pgduckdb::DuckDBQueryOrThrow(context, "ATTACH DATABASE ':memory:' AS pg_temp;");

	if (pgduckdb::IsMotherDuckEnabled()) {
		/*
		 * Workaround for MotherDuck catalog sync that turns off automatically,
		 * in case of no queries being sent to MotherDuck. Since the background
		 * worker never sends any query to MotherDuck we need to turn this off.
		 * So we set the timeout to an arbitrary very large value.
		 */
		pgduckdb::DuckDBQueryOrThrow(context,
		                             "SET motherduck_background_catalog_refresh_inactivity_timeout='99 years'");
	}

	LoadFunctions(context);
	LoadExtensions(context);
}

void
DuckDBManager::LoadFunctions(duckdb::ClientContext &context) {
	//auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
	context.transaction.BeginTransaction();
	auto &instance = *database->instance;
	duckdb::ExtensionUtil::RegisterType(instance, "UnsupportedPostgresType", duckdb::LogicalTypeId::VARCHAR);
	context.transaction.Commit();
}

int64
GetSeqLastValue(const char *seq_name) {
	Oid duckdb_namespace = get_namespace_oid("duckdb", false);
	Oid table_seq_oid = get_relname_relid(seq_name, duckdb_namespace);
	return PostgresFunctionGuard(DirectFunctionCall1Coll, pg_sequence_last_value, InvalidOid, table_seq_oid);
}

void
WriteSecretQueryForS3R2OrGCP(const DuckdbSecret &secret, std::ostringstream &query) {
	bool is_r2_cloud_secret = secret.type == SecretType::R2;
	query << "KEY_ID '" << secret.key_id << "', SECRET '" << secret.secret << "'";
	if (secret.region.length() && !is_r2_cloud_secret) {
		query << ", REGION '" << secret.region << "'";
	}
	if (secret.session_token.length() && !is_r2_cloud_secret) {
		query << ", SESSION_TOKEN '" << secret.session_token << "'";
	}
	if (secret.endpoint.length() && !is_r2_cloud_secret) {
		query << ", ENDPOINT '" << secret.endpoint << "'";
	}
	if (is_r2_cloud_secret) {
		query << ", ACCOUNT_ID '" << secret.endpoint << "'";
	}
	if (!secret.use_ssl) {
		query << ", USE_SSL 'FALSE'";
	}
	if (secret.scope.length()) {
		query << ", SCOPE '" << secret.scope << "'";
	}
}

void
DuckDBManager::LoadSecrets(duckdb::ClientContext &context) {
	auto duckdb_secrets = ReadDuckdbSecrets();

	int secret_id = 0;
	for (auto &secret : duckdb_secrets) {
		std::ostringstream query;
		query << "CREATE SECRET pgduckb_secret_" << secret_id << " ";
		query << "(TYPE " << SecretTypeToString(secret.type) << ", ";

		if (secret.type == SecretType::AZURE) {
			query << "CONNECTION_STRING '" << secret.connection_string << "'";
		} else {
			WriteSecretQueryForS3R2OrGCP(secret, query);
		}

		query << ");";

		DuckDBQueryOrThrow(context, query.str());
		secret_id++;
		secret_table_num_rows = secret_id;
	}
}

void
DuckDBManager::DropSecrets(duckdb::ClientContext &context) {
	for (auto secret_id = 0; secret_id < secret_table_num_rows; ++secret_id) {
		auto drop_secret_cmd = duckdb::StringUtil::Format("DROP SECRET pgduckb_secret_%d;", secret_id);
		pgduckdb::DuckDBQueryOrThrow(context, drop_secret_cmd);
	}

	secret_table_num_rows = 0;
}

void
DuckDBManager::LoadExtensions(duckdb::ClientContext &context) {
	auto duckdb_extensions = ReadDuckdbExtensions();

	for (auto &extension : duckdb_extensions) {
		if (!extension.enabled) {
			continue;
		}

		/*
		 * Skip the httpfs extension. It conflicts with our cached_httpfs
		 * extension which is loaded by default, but people might still try to
		 * install it manually. We could also throw an error by doing so, but
		 * it seems nicer to just skip it, because it is clear what they meant
		 * to do and this way we don't need to educate people about the
		 * existence of our cached_httpfs extension (which might be removed in
		 * the near future anyway).
		 */
		if (extension.name == "httpfs") {
			continue;
		}

		DuckDBQueryOrThrow(context, "LOAD " + extension.name);
	}
}

void
DuckDBManager::RefreshConnectionState(duckdb::ClientContext &context) {
	const auto extensions_table_last_seq = GetSeqLastValue("extensions_table_seq");
	if (IsExtensionsSeqLessThan(extensions_table_last_seq)) {
		LoadExtensions(context);
		UpdateExtensionsSeq(extensions_table_last_seq);
	}

	const auto secret_table_last_seq = GetSeqLastValue("secrets_table_seq");
	if (IsSecretSeqLessThan(secret_table_last_seq)) {
		DropSecrets(context);
		LoadSecrets(context);
		UpdateSecretSeq(secret_table_last_seq);
	}

	auto http_file_cache_set_dir_query =
	    duckdb::StringUtil::Format("SET http_file_cache_dir TO '%s';", CreateOrGetDirectoryPath("duckdb_cache"));
	DuckDBQueryOrThrow(context, http_file_cache_set_dir_query);

	if (duckdb_disabled_filesystems != NULL && !superuser()) {
		/*
		 * DuckDB does not allow us to disable this setting on the
		 * database after the DuckDB connection is created for a non
		 * superuser, any further connections will inherit this
		 * restriction. This means that once a non-superuser used a
		 * DuckDB connection in aside a Postgres backend, any loter
		 * connection will inherit these same filesystem restrictions.
		 * This shouldn't be a problem in practice.
		 */
		pgduckdb::DuckDBQueryOrThrow(context,
		                             "SET disabled_filesystems='" + std::string(duckdb_disabled_filesystems) + "'");
	}
}

/*
 * Creates a new connection to the global DuckDB instance. This should only be
 * used in some rare cases, where a temporary new connection is needed instead
 * of the global cached connection that is returned by GetConnection.
 */
duckdb::unique_ptr<duckdb::Connection>
DuckDBManager::CreateConnection() {
	if (!pgduckdb::IsDuckdbExecutionAllowed()) {
		elog(ERROR, "DuckDB execution is not allowed because you have not been granted the duckdb.postgres_role");
	}

	auto &instance = Get();
	auto connection = duckdb::make_uniq<duckdb::Connection>(*instance.database);
	auto &context = *connection->context;

	instance.RefreshConnectionState(context);

	return connection;
}

/* Returns the cached connection to the global DuckDB instance. */
duckdb::Connection *
DuckDBManager::GetConnection(bool force_transaction) {
	if (!pgduckdb::IsDuckdbExecutionAllowed()) {
		elog(ERROR, "DuckDB execution is not allowed because you have not been granted the duckdb.postgres_role");
	}

	auto &instance = Get();
	auto &context = *instance.connection->context;

	if (!context.transaction.HasActiveTransaction()) {
		if (IsSubTransaction()) {
			throw duckdb::NotImplementedException("SAVEPOINT and subtransactions are not supported in DuckDB");
		}

		if (force_transaction || pg::IsInTransactionBlock()) {
			/*
			 * We only want to open a new DuckDB transaction if we're already
			 * in a Postgres transaction block. Always opening a transaction
			 * incurs a significant performance penalty for single statement
			 * queries on MotherDuck. This is because a second round-trip is
			 * needed to send the COMMIT to MotherDuck when Postgres its
			 * transaction finishes. So we only want to do this when actually
			 * necessary.
			 */
			instance.connection->BeginTransaction();
		}
	}

	instance.RefreshConnectionState(context);

	return instance.connection.get();
}

/*
 * Returns the cached connection to the global DuckDB instance, but does not do
 * any checks required to correctly initialize the DuckDB transaction nor
 * refreshes the secrets/extensions/etc. Only use this in rare cases where you
 * know for sure that the connection is already initialized for the correctly
 * for the current query, and you just want a pointer to it.
 */
duckdb::Connection *
DuckDBManager::GetConnectionUnsafe() {
	auto &instance = Get();
	return instance.connection.get();
}

} // namespace pgduckdb
