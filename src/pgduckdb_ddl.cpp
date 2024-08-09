#include "duckdb.hpp"
#include <regex>

extern "C" {
#include "postgres.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "commands/event_trigger.h"
#include "fmgr.h"
#include "catalog/pg_authid_d.h"
#include "funcapi.h"
#include "nodes/print.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "tcop/utility.h"
#include "utils/syscache.h"
#include "commands/event_trigger.h"
#include "executor/spi.h"
#include "miscadmin.h"
}

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_pg_list.h"

void
duckdb_handle_ddl(Node *parsetree, const char *queryString) {
	if (IsA(parsetree, CreateTableAsStmt)) {
		auto stmt = (CreateTableAsStmt *)parsetree;
		char *access_method = stmt->into->accessMethod ? stmt->into->accessMethod : default_table_access_method;
		// TODO: Do something special here to make sure that the select doesn't
		// insert into the postgres table too.
	}
}

extern "C" {

PG_FUNCTION_INFO_V1(duckdb_create_table_trigger);

Datum
duckdb_create_table_trigger(PG_FUNCTION_ARGS) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	auto trigdata = (EventTriggerData *)fcinfo->context;

	SPI_connect();

	/* Temporarily escalate privileges to superuser so we can insert into duckdb.tables */
	Oid saved_userid;
	int sec_context;
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);
	/*
	 * We track the table oid in duckdb.tables so we can later check in our
	 * delete event trigger if the table was created using the duckdb access
	 * method. See the code comment in that function for more details.
	 */
	int ret = SPI_exec(R"(
                INSERT INTO duckdb.tables(relid)
                SELECT objid
                FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
                JOIN pg_catalog.pg_class
                ON cmds.objid = pg_class.oid
                WHERE cmds.object_type = 'table'
                AND pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'duckdb'))",
	                   0);

	/* Revert back to original privileges */
	SetUserIdAndSecContext(saved_userid, sec_context);

	if (ret != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

	/* if we inserted a row it was a duckdb table */
	auto isDuckdbTable = SPI_processed > 0;

	SPI_finish();

	if (!isDuckdbTable) {
		PG_RETURN_NULL();
	}

	// TODO: This is a huge hack, we should build the query string from the parsetree
	std::string query_string = std::regex_replace(debug_query_string, std::regex(R"((using|USING)\s+duckdb)"), "");

	auto db = pgduckdb::DuckDBManager::Get().GetDatabase();
	auto connection = duckdb::make_uniq<duckdb::Connection>(db);
	auto result = pgduckdb::RunQuery(*connection, query_string);

	PG_RETURN_NULL();
}
PG_FUNCTION_INFO_V1(duckdb_drop_table_trigger);

Datum
duckdb_drop_table_trigger(PG_FUNCTION_ARGS) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	auto trigdata = (EventTriggerData *)fcinfo->context;

	SPI_connect();

	/* Temporarily escalate privileges to superuser so we can delete from duckdb.tables */
	Oid saved_userid;
	int sec_context;
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);

	// Because the table metadata is deleted from the postgres catalogs we
	// cannot find out if the table was using the duckdb access method. So
	// instead we keep our own metadata table that also tracks which tables are
	// duckdb tables.
	// TODO: Handle schemas in a sensible way, by using object_identity instead
	// of object_name
	int ret = SPI_exec(R"(
                DELETE FROM duckdb.tables
                USING (
                    SELECT objid, object_name, object_identity
                    FROM pg_catalog.pg_event_trigger_dropped_objects()
                    WHERE object_type = 'table'
                ) objs
                WHERE relid = objid
                RETURNING objs.object_name
                )",
	                   0);

	/* Revert back to original privileges */
	SetUserIdAndSecContext(saved_userid, sec_context);

	if (ret != SPI_OK_DELETE_RETURNING)
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

	auto db = pgduckdb::DuckDBManager::Get().GetDatabase();
	auto connection = duckdb::make_uniq<duckdb::Connection>(db);

	for (auto proc = 0; proc < SPI_processed; proc++) {
		HeapTuple tuple = SPI_tuptable->vals[proc];

		char *object_identity = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
		// TODO: Do this in a transaction
		pgduckdb::RunQuery(*connection, "DROP TABLE IF EXISTS " + std::string(object_identity));
	}

	SPI_finish();

	PG_RETURN_NULL();
}
}
