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

#include "pgduckdb/pgduckdb_ruleutils.h"
}

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include <inttypes.h>

/*
 * Truncates the given table in DuckDB.
 */
void
DuckdbTruncateTable(Oid relation_oid) {
	auto name = pgduckdb::PostgresFunctionGuard<char *>(pgduckdb_relation_name, relation_oid);
	pgduckdb::DuckDBQueryOrThrow(std::string("TRUNCATE ") + name);
}

void
DuckdbHandleDDL(Node *parsetree, const char *queryString) {
	if (IsA(parsetree, CreateTableAsStmt)) {
		auto stmt = castNode(CreateTableAsStmt, parsetree);
		char *access_method = stmt->into->accessMethod ? stmt->into->accessMethod : default_table_access_method;
		if (strcmp(access_method, "duckdb") != 0) {
			/* not a duckdb table, so don't mess with the query */
			return;
		}

		elog(ERROR, "DuckDB does not support CREATE TABLE AS yet");
	}
}

/*
 * Throws an error when an unsupported ON COMMIT clause is used. DuckDB does
 * not support ON COMMIT DROP, and it's difficult to emulate because Postgres
 * does not call any hooks or event triggers when dropping a table due to an ON
 * COMMIT DROP clause. We could simplify the below check to only check for
 * ONCOMMIT_DROP, but this will also handle any new ON COMMIT clauses that
 * might be added to Postgres in future releases.
 */
void
CheckOnCommitSupport(OnCommitAction on_commit) {
	switch (on_commit) {
	case ONCOMMIT_NOOP:
	case ONCOMMIT_PRESERVE_ROWS:
	case ONCOMMIT_DELETE_ROWS:
		break;
	case ONCOMMIT_DROP:
		elog(ERROR, "DuckDB does not support ON COMMIT DROP");
	default:
		elog(ERROR, "Unsupported ON COMMIT clause: %d", on_commit);
	}
}

extern "C" {

PG_FUNCTION_INFO_V1(duckdb_create_table_trigger);
Datum
duckdb_create_table_trigger(PG_FUNCTION_ARGS) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	EventTriggerData *trigdata = (EventTriggerData *)fcinfo->context;
	Node *parsetree = trigdata->parsetree;

	SPI_connect();

	/*
	 * Temporarily escalate privileges to superuser so we can insert into
	 * duckdb.tables. We temporarily clear the search_path to protect against
	 * search_path confusion attacks. See guidance on SECURITY DEFINER
	 * functions in postgres for details:
	 * https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY
	 */
	Oid saved_userid;
	int sec_context;
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);
	/*
	 * We track the table oid in duckdb.tables so we can later check in our
	 * delete event trigger if the table was created using the duckdb access
	 * method. See the code comment in that function for more details.
	 */
	int ret = SPI_exec(R"(
		INSERT INTO duckdb.tables(relid)
		SELECT DISTINCT objid
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN pg_catalog.pg_class
		ON cmds.objid = pg_class.oid
		WHERE cmds.object_type = 'table'
		AND pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'duckdb')
		RETURNING relid)",
	                   0);

	/* Revert back to original privileges */
	SetUserIdAndSecContext(saved_userid, sec_context);
	AtEOXact_GUC(false, save_nestlevel);

	if (ret != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

	/* if we inserted a row it was a duckdb table */
	auto is_duckdb_table = SPI_processed > 0;
	if (!is_duckdb_table) {
		SPI_finish();
		PG_RETURN_NULL();
	}

	if (SPI_processed != 1) {
		elog(ERROR, "Expected single table to be created, but found %" PRIu64, SPI_processed);
	}

	HeapTuple tuple = SPI_tuptable->vals[0];
	bool isnull;
	Datum relid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull) {
		elog(ERROR, "Expected relid to be returned, but found NULL");
	}

	Oid relid = DatumGetObjectId(relid_datum);
	SPI_finish();

	/*
	 * For now, we don't support DuckDB queries in transactions. To support
	 * write queries in transactions we'll need to link Postgres and DuckdB
	 * their transaction lifecycles.
	 */
	PreventInTransactionBlock(true, "DuckDB queries");

	if (IsA(parsetree, CreateStmt)) {
		auto stmt = castNode(CreateStmt, parsetree);
		CheckOnCommitSupport(stmt->oncommit);
	} else if (IsA(parsetree, CreateTableAsStmt)) {
		auto stmt = castNode(CreateTableAsStmt, parsetree);
		CheckOnCommitSupport(stmt->into->onCommit);
	} else {
		elog(ERROR, "Unexpected parsetree type: %d", nodeTag(parsetree));
	}

	pgduckdb::DuckDBQueryOrThrow(pgduckdb_get_tabledef(relid));
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(duckdb_drop_table_trigger);
Datum
duckdb_drop_table_trigger(PG_FUNCTION_ARGS) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	SPI_connect();

	/*
	 * Temporarily escalate privileges to superuser so we can insert into
	 * duckdb.tables. We temporarily clear the search_path to protect against
	 * search_path confusion attacks. See guidance on SECURITY DEFINER
	 * functions in postgres for details:
	 * https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY
	 */
	Oid saved_userid;
	int sec_context;
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);

	// Because the table metadata is deleted from the postgres catalogs we
	// cannot find out if the table was using the duckdb access method. So
	// instead we keep our own metadata table that also tracks which tables are
	// duckdb tables.
	int ret = SPI_exec(R"(
		DELETE FROM duckdb.tables
		USING (
			SELECT objid, object_name, object_identity
			FROM pg_catalog.pg_event_trigger_dropped_objects()
			WHERE object_type = 'table'
		) objs
		WHERE relid = objid
		RETURNING objs.object_identity
		)",
	                   0);

	/* Revert back to original privileges */
	SetUserIdAndSecContext(saved_userid, sec_context);
	AtEOXact_GUC(false, save_nestlevel);

	if (ret != SPI_OK_DELETE_RETURNING)
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

	if (SPI_processed == 0) {
		/* No duckdb tables were dropped */
		SPI_finish();
		PG_RETURN_NULL();
	}

	/*
	 * For now, we don't support DuckDB queries in transactions. To support
	 * write queries in transactions we'll need to link Postgres and DuckdB
	 * their transaction lifecycles.
	 */
	PreventInTransactionBlock(true, "DuckDB queries");

	auto connection = pgduckdb::DuckDBManager::CreateConnection();

	pgduckdb::DuckDBQueryOrThrow(*connection, "BEGIN TRANSACTION");

	for (auto proc = 0; proc < SPI_processed; ++proc) {
		HeapTuple tuple = SPI_tuptable->vals[proc];

		char *object_identity = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
		// TODO: Handle std::string creation in a safe way for allocation failures
		pgduckdb::DuckDBQueryOrThrow(*connection, "DROP TABLE IF EXISTS " + std::string(object_identity));
	}

	SPI_finish();

	pgduckdb::DuckDBQueryOrThrow(*connection, "COMMIT");
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(duckdb_alter_table_trigger);
Datum
duckdb_alter_table_trigger(PG_FUNCTION_ARGS) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	SPI_connect();

	/*
	 * Temporarily escalate privileges to superuser so we can insert into
	 * duckdb.tables. We temporarily clear the search_path to protect against
	 * search_path confusion attacks. See guidance on SECURITY DEFINER
	 * functions in postgres for details:
	 * https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY
	 */
	Oid saved_userid;
	int sec_context;
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);
	/*
	 * Check if a table was altered that was created using the duckdb
	 * access. This needs to check both pg_class and duckdb.tables, because the
	 * access method might have been changed from/to duckdb by the ALTER TABLE
	 * SET ACCESS METHOD command.
	 */
	int ret = SPI_exec(R"(
		SELECT objid as relid
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN pg_catalog.pg_class
		ON cmds.objid = pg_class.oid
		WHERE cmds.object_type = 'table'
		AND pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'duckdb')
		UNION ALL
		SELECT objid as relid
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN duckdb.tables AS ddbtables
		ON cmds.objid = ddbtables.relid
		WHERE cmds.object_type = 'table'
		)",
	                   0);

	/* Revert back to original privileges */
	SetUserIdAndSecContext(saved_userid, sec_context);
	AtEOXact_GUC(false, save_nestlevel);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

	/* if we inserted a row it was a duckdb table */
	auto is_duckdb_table = SPI_processed > 0;
	if (!is_duckdb_table) {
		SPI_finish();
		PG_RETURN_NULL();
	}

	elog(ERROR, "DuckDB does not support ALTER TABLE yet");

	PG_RETURN_NULL();
}
}
