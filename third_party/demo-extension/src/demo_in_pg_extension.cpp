#include "duckdb/common/helper.hpp"
#define DUCKDB_EXTENSION_MAIN

#include "demo_in_pg_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

extern "C" {
#undef ENABLE_NLS
#include "postgres.h"
#include "catalog/pg_authid_d.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "executor/spi.h"
#include "miscadmin.h"

// Postgres overrides printf - we don't want that
#ifdef printf
#undef printf
#endif

} // extern "C"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

extern int PostmasterPid __attribute__((weak));
extern bool IsUnderPostmaster __attribute__((weak));

typedef void (*ExecutorRun_hook_type)(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once);

extern ExecutorRun_hook_type ExecutorRun_hook __attribute__((weak));

extern void standard_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once)
    __attribute__((weak));

static ExecutorRun_hook_type PrevExecutorRunHook = NULL;

static emit_log_hook_type prev_emit_log_hook = NULL;

namespace duckdb {

inline void
DemoInPgScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "DemoInPg >> " + name.GetString() + "<< 🐥");
		;
	});
}

inline void
DemoInPgOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	elog(NOTICE, "Initializing 'demo_in_pg' extension -- demo_in_pg_version\n");
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "DemoInPg " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
		;
	});
}

inline void
SyncCatalogsWithPg(DataChunk &args, ExpressionState &state, Vector &result) {
	SPI_connect();

	/* Temporarily escalate privileges to superuser so we can insert into quack.tables */
	Oid saved_userid;
	int sec_context;
	elog(NOTICE, "Syncing duckdb catalogs with postgres");
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);
	int ret = SPI_exec(R"(
                    SELECT relname from pg_class limit 5
                    )",
	                   0);

	/* Revert back to original privileges */
	SetUserIdAndSecContext(saved_userid, sec_context);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: error code %d", ret);

	for (auto proc = 0; proc < SPI_processed; proc++) {
		HeapTuple tuple = SPI_tuptable->vals[proc];
		auto relname = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
	}
	SPI_finish();
}

struct DuckDBTablesData : public GlobalTableFunctionState {
	DuckDBTablesData() : offset(0) {
	}

	vector<reference<CatalogEntry>> entries;
	idx_t offset;
};

static unique_ptr<FunctionData>
DuckDBTablesBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                 vector<string> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("comment");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState>
DuckDBTablesInit2(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBTablesData>();

	// scan all the schemas for tables and collect themand collect them
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::TABLE_ENTRY,
		                  [&](CatalogEntry &entry) { result->entries.push_back(entry); });
	};
	return std::move(result);
}

static bool
TableHasPrimaryKey(TableCatalogEntry &table) {
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type == ConstraintType::UNIQUE) {
			auto &unique = constraint->Cast<UniqueConstraint>();
			if (unique.IsPrimaryKey()) {
				return true;
			}
		}
	}
	return false;
}

static idx_t
CheckConstraintCount(TableCatalogEntry &table) {
	idx_t check_count = 0;
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type == ConstraintType::CHECK) {
			check_count++;
		}
	}
	return check_count;
}

PgCreateTableInfo::PgCreateTableInfo(unique_ptr<CreateTableInfo> info) {
	CopyProperties(*info);
	table = info->table;
	columns = info->columns.Copy();
	for (auto &constraint : constraints) {
		constraints.push_back(constraint->Copy());
	}
}

string
PgCreateTableInfo::ToString() const {
	string ret = "";

	ret += "CREATE";
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		ret += " OR REPLACE";
	}
	if (temporary) {
		ret += " TEMP";
	}
	ret += " TABLE ";

	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ret += " IF NOT EXISTS ";
	}
	// TODO: Do something with the catalog and schema
	ret += KeywordHelper::WriteOptionallyQuoted(table);

	ret += TableCatalogEntry::ColumnsToSQL(columns, constraints);
	ret += " USING duckdb;";
	return ret;
}

void
DuckDBTablesFunction2(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBTablesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	SPI_connect();
	Oid saved_userid;
	int sec_context;
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);
	int ret = SPI_exec("ALTER EVENT TRIGGER quack_create_table_trigger DISABLE", 0);

	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "SPI_execute failed: error code %d", ret);

	/* Revert back to original privileges */
	SetUserIdAndSecContext(saved_userid, sec_context);
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++].get();

		if (entry.type != CatalogType::TABLE_ENTRY) {
			continue;
		}
		auto &table = entry.Cast<TableCatalogEntry>();
		auto storage_info = table.GetStorageInfo(context);
		// return values:
		idx_t col = 0;
		// database_name, VARCHAR
		output.SetValue(col++, count, table.catalog.GetName());
		// schema_name, LogicalType::VARCHAR
		output.SetValue(col++, count, Value(table.schema.name));
		// table_name, LogicalType::VARCHAR
		output.SetValue(col++, count, Value(table.name));
		// comment, LogicalType::VARCHAR
		output.SetValue(col++, count, Value(table.comment));

		// sql, LogicalType::VARCHAR
		// unique_ptr<CreateInfo> table_info = table.GetInfo();
		auto table_info = unique_ptr_cast<CreateInfo, CreateTableInfo>(table.GetInfo());
		auto pg_table_info = make_uniq<PgCreateTableInfo>(std::move(table_info));
		pg_table_info->catalog = table.catalog.GetName();
		pg_table_info->schema = table.schema.name;
		output.SetValue(col++, count, Value(pg_table_info->ToString()));

		int ret = SPI_exec(pg_table_info->ToString().c_str(), 0);

		if (ret != SPI_OK_UTILITY)
			elog(ERROR, "SPI_execute failed: error code %d", ret);

		count++;
	}
	ret = SPI_exec("ALTER EVENT TRIGGER quack_create_table_trigger ENABLE", 0);

	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "SPI_execute failed: error code %d", ret);
	SPI_finish();
	output.SetCardinality(count);
}

static void
columnar_executor_run(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once) {
	// elog(INFO, "[demo_in_pg] Hey lookaaaaaaaaaaaaaaaaaaaaaaa! I'm in the columnar_executor_run hook!");

	if (PrevExecutorRunHook) {
		PrevExecutorRunHook(queryDesc, direction, count, execute_once);
	}
}

void
columnar_emit_log_hook(::ErrorData *edata) {
	if (prev_emit_log_hook) {
		prev_emit_log_hook(edata);
	}

	printf("[demo_in_pg] got message: '%s'\n", edata->message);
}

static void
LoadInternal(DatabaseInstance &instance) {
	if (&PostmasterPid) {
		elog(INFO, "[demo_in_pg] This DuckDB extension is running in Postgres, will hook into the logger");
		// Since we load DuckDB twice don't chain the hooks with our prior self :-)
		if (ExecutorRun_hook != columnar_executor_run) {
			PrevExecutorRunHook = ExecutorRun_hook ? ExecutorRun_hook : standard_ExecutorRun;
			ExecutorRun_hook = columnar_executor_run;

			prev_emit_log_hook = emit_log_hook ? emit_log_hook : NULL;
			emit_log_hook = columnar_emit_log_hook;
		}

	} else {
		printf("This extension is NOT running in Postgres\n");
	}

	// Register a scalar function
	auto demo_in_pg_scalar_function =
	    ScalarFunction("demo_in_pg", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DemoInPgScalarFun);
	ExtensionUtil::RegisterFunction(instance, demo_in_pg_scalar_function);

	// Register another scalar function
	auto demo_in_pg_openssl_version_scalar_function = ScalarFunction(
	    "demo_in_pg_openssl_version", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DemoInPgOpenSSLVersionScalarFun);
	ExtensionUtil::RegisterFunction(instance, demo_in_pg_openssl_version_scalar_function);

	auto duckdb_tables_function =
	    TableFunction("sync_catalogs_with_pg", {}, DuckDBTablesFunction2, DuckDBTablesBind, DuckDBTablesInit2);
	ExtensionUtil::RegisterFunction(instance, duckdb_tables_function);
}

void
DemoInPgExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string
DemoInPgExtension::Name() {
	return "demo_in_pg";
}

std::string
DemoInPgExtension::Version() const {
#ifdef EXT_VERSION_DEMO_IN_PG
	return EXT_VERSION_DEMO_IN_PG;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void
demo_in_pg_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::DemoInPgExtension>();
}

DUCKDB_EXTENSION_API const char *
demo_in_pg_version() {
	printf("Initializing 'demo_in_pg' extension -- demo_in_pg_version\n");
	elog(NOTICE, "Initializing 'demo_in_pg' extension -- demo_in_pg_version\n");
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
