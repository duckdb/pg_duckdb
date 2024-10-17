#include "duckdb.hpp"
#include <regex>

extern "C" {
#include "postgres.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "commands/event_trigger.h"
#include "fmgr.h"
#include "catalog/pg_authid_d.h"
#include "catalog/namespace.h"
#include "funcapi.h"
#include "nodes/print.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "commands/event_trigger.h"
#include "executor/spi.h"
#include "miscadmin.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
#include "pgduckdb/pgduckdb_ruleutils.h"
}

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include <inttypes.h>

/*
 * ctas_skip_data stores the original value of the skipData field of the
 * CreateTableAsStmt of the query that's currently being executed. For duckdb
 * tables we force this value to false.
 */
static bool ctas_skip_data = false;

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
	if (!pgduckdb::IsExtensionRegistered()) {
		/* We're not installed, so don't mess with the query */
		return;
	}

	if (IsA(parsetree, CreateTableAsStmt)) {
		auto stmt = castNode(CreateTableAsStmt, parsetree);
		char *access_method = stmt->into->accessMethod ? stmt->into->accessMethod : default_table_access_method;
		if (strcmp(access_method, "duckdb") != 0) {
			/* not a duckdb table, so don't mess with the query */
			return;
		}

		/*
		 * Force skipData to false for duckdb tables, so that Postgres does
		 * not execute the query, and save the original value in ctas_skip_data
		 * so we can use it later in duckdb_create_table_trigger to choose
		 * whether to execute the query in DuckDB or not.
		 */
		ctas_skip_data = stmt->into->skipData;
		stmt->into->skipData = true;
	} else if (IsA(parsetree, CreateSchemaStmt) && !pgduckdb::doing_motherduck_sync) {
		auto stmt = castNode(CreateSchemaStmt, parsetree);
		if (strncmp("ddb$", stmt->schemaname, 4) == 0) {
			elog(ERROR, "Creating ddb$ schemas is currently not supported");
		}
		return;
	} else if (IsA(parsetree, RenameStmt)) {
		auto stmt = castNode(RenameStmt, parsetree);
		if (stmt->renameType != OBJECT_SCHEMA) {
			/* We only care about schema renames for now */
			return;
		}
		if (strncmp("ddb$", stmt->subname, 4) == 0) {
			elog(ERROR, "Changing the name of a ddb$ schema is currently not supported");
		}
		if (strncmp("ddb$", stmt->newname, 4) == 0) {
			elog(ERROR, "Changing a schema to a ddb$ schema is currently not supported");
		}
		return;
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
		break;
	default:
		elog(ERROR, "Unsupported ON COMMIT clause: %d", on_commit);
	}
}

extern "C" {

/*
 * Stores the oids of temporary DuckDB tables for this backend. We cannot store
 * these Oids in the duckdb.tables table. This is because these tables are
 * automatically dropped when the backend terminates, but for this type of drop
 * no event trigger is fired. So if we would store these Oids in the
 * duckdb.tables table, then the oids of temporary tables would stay in there
 * after the backend terminates (which would be bad, because the table doesn't
 * exist anymore). To solve this, we store the oids in this in-memory set
 * instead, because that memory will automatically be cleared when the current
 * backend terminates.
 */
static std::unordered_set<Oid> temporary_duckdb_tables;

PG_FUNCTION_INFO_V1(duckdb_create_table_trigger);
Datum
duckdb_create_table_trigger(PG_FUNCTION_ARGS) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	if (!pgduckdb::IsExtensionRegistered()) {
		/*
		 * We're not installed, so don't mess with the query. Normally this
		 * shouldn't happen, but better safe than sorry.
		 */
		PG_RETURN_NULL();
	}

	EventTriggerData *trigger_data = (EventTriggerData *)fcinfo->context;
	Node *parsetree = trigger_data->parsetree;

	SPI_connect();

	/*
	 * We temporarily escalate privileges to superuser for some of our queries
	 * so we can insert into duckdb.tables. We temporarily clear the
	 * search_path to protect against search_path confusion attacks. See
	 * guidance on SECURITY DEFINER functions in postgres for details:
	 * https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY
	 *
	 * We also temporarily force duckdb.execution to false, because
	 * pg_catalog.pg_event_trigger_ddl_commands does not exist in DuckDB.
	 */
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("duckdb.execution", "false", PGC_USERSET, PGC_S_SESSION);

	int ret = SPI_exec(R"(
		SELECT DISTINCT objid AS relid, pg_class.relpersistence = 't' AS is_temporary
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN pg_catalog.pg_class
		ON cmds.objid = pg_class.oid
		WHERE cmds.object_type = 'table'
		AND pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'duckdb')
		)",
	                   0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

	/* if we selected a row it was a duckdb table */
	auto is_duckdb_table = SPI_processed > 0;
	if (!is_duckdb_table) {
		/* No DuckDB tables were created so we don't need to do anything */
		AtEOXact_GUC(false, save_nestlevel);
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
	Datum is_temporary_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull);
	if (isnull) {
		elog(ERROR, "Expected temporary boolean to be returned, but found NULL");
	}

	Oid relid = DatumGetObjectId(relid_datum);
	bool is_temporary = DatumGetBool(is_temporary_datum);

	/*
	 * We track the table oid in duckdb.tables so we can later check in our
	 * duckdb_drop_trigger if the table was created using the duckdb access
	 * method. See the code comment in that function and the one on
	 * temporary_duckdb_tables for more details.
	 */
	if (is_temporary) {
		temporary_duckdb_tables.insert(relid);
	} else {
		Oid saved_userid;
		int sec_context;
		const char *postgres_schema_name = get_namespace_name_or_temp(get_rel_namespace(relid));
		const char *duckdb_db = (const char *)linitial(pgduckdb_db_and_schema(postgres_schema_name, true));
		GetUserIdAndSecContext(&saved_userid, &sec_context);
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);

		Oid arg_types[] = {OIDOID, TEXTOID, TEXTOID};
		Datum values[] = {relid_datum, CStringGetTextDatum(duckdb_db), 0};
		char nulls[] = {' ', ' ', 'n'};

		if (pgduckdb::doing_motherduck_sync) {
			values[2] = CStringGetTextDatum(pgduckdb::current_motherduck_catalog_version);
			nulls[2] = ' ';
		}
		ret = SPI_execute_with_args(R"(
			INSERT INTO duckdb.tables (relid, duckdb_db, motherduck_catalog_version)
			VALUES ($1, $2, $3)
			)",
		                            lengthof(arg_types), arg_types, values, nulls, false, 0);

		/* Revert back to original privileges */
		SetUserIdAndSecContext(saved_userid, sec_context);

		if (ret != SPI_OK_INSERT)
			elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
	}

	AtEOXact_GUC(false, save_nestlevel);
	SPI_finish();

	if (pgduckdb::doing_motherduck_sync) {
		/*
		 * We don't want to forward DDL to DuckDB because we're syncing the
		 * tables that are already there.
		 */
		PG_RETURN_NULL();
	}

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

	/*
	 * pgduckdb_get_tabledef does a bunch of checks to see if creating the
	 * table is supported. So, we do call that function first, before creating
	 * the DuckDB connection and possibly transactions.
	 */
	std::string create_table_string(pgduckdb_get_tabledef(relid));

	auto connection = pgduckdb::DuckDBManager::Get().CreateConnection();
	Query *ctas_query = nullptr;

	if (IsA(parsetree, CreateTableAsStmt) && !ctas_skip_data) {
		auto stmt = castNode(CreateTableAsStmt, parsetree);
		ctas_query = (Query *)stmt->query;
	}

	if (ctas_query) {
		pgduckdb::DuckDBQueryOrThrow(*connection, "BEGIN TRANSACTION");
	}

	pgduckdb::DuckDBQueryOrThrow(*connection, create_table_string);
	if (ctas_query) {

		const char *ctas_query_string = pgduckdb_get_querydef(ctas_query);

		std::string insert_string =
		    std::string("INSERT INTO ") + pgduckdb_relation_name(relid) + " " + ctas_query_string;
		pgduckdb::DuckDBQueryOrThrow(*connection, insert_string);

		pgduckdb::DuckDBQueryOrThrow(*connection, "COMMIT");
	}

	PG_RETURN_NULL();
}

/*
 * We use the sql_drop event trigger to handle drops of DuckDB tables and
 * schemas because drops might cascade to other objects, so just checking the
 * DROP SCHEMA and DROP TABLE commands is not enough. We could ofcourse
 * manually resolve the dependencies for every drop command, but by using the
 * sql_drop trigger that is already done for us.
 *
 * The main downside of using the sql_drop trigger is that when this trigger is
 * executed, the objects are already dropped. That means that it's not possible
 * to query the Postgres catalogs for information about them, which means that
 * we need to do some extra bookkeeping ourselves to be able to figure out if a
 * dropped table was a DuckDB table or not (because we cannot check its access
 * method anymore).
 */
PG_FUNCTION_INFO_V1(duckdb_drop_trigger);
Datum
duckdb_drop_trigger(PG_FUNCTION_ARGS) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	if (!pgduckdb::IsExtensionRegistered()) {
		/*
		 * We're not installed, so don't mess with the query. Normally this
		 * shouldn't happen, but better safe than sorry.
		 */
		PG_RETURN_NULL();
	}

	SPI_connect();

	/*
	 * Temporarily escalate privileges to superuser so we can insert into
	 * duckdb.tables. We temporarily clear the search_path to protect against
	 * search_path confusion attacks. See guidance on SECURITY DEFINER
	 * functions in postgres for details:
	 * https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY
	 *
	 * We also temporarily force duckdb.execution to false, because
	 * pg_catalog.pg_event_trigger_dropped_objects does not exist in DuckDB.
	 */
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("duckdb.execution", "false", PGC_USERSET, PGC_S_SESSION);

	if (!pgduckdb::doing_motherduck_sync) {
		int ret = SPI_exec(R"(
			SELECT object_identity
			FROM pg_catalog.pg_event_trigger_dropped_objects()
			WHERE object_type = 'schema'
				AND object_identity LIKE 'ddb$%'
			)",
		                   0);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

		if (SPI_processed > 0) {
			elog(ERROR, "Currently it's not possible to drop ddb$ schemas");
		}
	}

	Oid saved_userid;
	int sec_context;
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);
	/*
	 * Because the table metadata is deleted from the postgres catalogs we
	 * cannot find out if the table was using the duckdb access method. So
	 * instead we keep our own metadata table that also tracks which tables are
	 * duckdb tables. We do the same for temporary tables, except we use an
	 * in-memory set for that. See the comment on the temporary_duckdb_tables
	 * global for details on why.
	 *
	 * Here we first handle the non-temporary tables.
	 */

	int ret = SPI_exec(R"(
		DELETE FROM duckdb.tables
		USING (
			SELECT objid, schema_name, object_name
			FROM pg_catalog.pg_event_trigger_dropped_objects()
			WHERE object_type = 'table'
		) objs
		WHERE relid = objid
		RETURNING objs.schema_name, objs.object_name
		)",
	                   0);

	/* Revert back to original privileges */
	SetUserIdAndSecContext(saved_userid, sec_context);

	if (ret != SPI_OK_DELETE_RETURNING)
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

	/*
	 * We lazily create a connection to DuckDB when we need it. That's
	 * important because we don't want to impose our "prevent in transaction
	 * block" restriction unnecessarily. Otherwise users won't be able to drop
	 * regular heap tables in transactions anymore.
	 */
	duckdb::unique_ptr<duckdb::Connection> connection;

	/*
	 * Now forward the DROP to DuckDB... but only if MotherDuck is actually
	 * enabled. It's possible for MotherDuck tables to exist in Postgres exist
	 * even if MotherDuck is disabled. This can happen when people reconfigure
	 * their Postgres settings to disable MotherDuck after first having it
	 * enabled. In that case we don't automatically drop the orphan tables in
	 * Postgres. By not forwarding the DROP to DuckDB we allow people to
	 * manually clean them up.
	 *
	 * It's also extremely important that we don't forward the DROP to DuckDB
	 * if we're currently syncing the MotherDuck catalog. Otherwise we would
	 * actually cause the tables to be dropped in MotherDuck as well, even if
	 * the DROP is only meant to replace the existing Postgres shell table with
	 * a new version.
	 */
	if (pgduckdb::IsMotherDuckEnabled() && !pgduckdb::doing_motherduck_sync) {
		for (auto proc = 0; proc < SPI_processed; ++proc) {
			if (!connection) {
				/*
				 * For now, we don't support DuckDB queries in transactions. To support
				 * write queries in transactions we'll need to link Postgres and DuckdB
				 * their transaction lifecycles.
				 */
				PreventInTransactionBlock(true, "DuckDB queries");
				connection = pgduckdb::DuckDBManager::CreateConnection();
				pgduckdb::DuckDBQueryOrThrow(*connection, "BEGIN TRANSACTION");
			}
			HeapTuple tuple = SPI_tuptable->vals[proc];

			char *postgres_schema_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
			char *table_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);
			char *drop_query = psprintf("DROP TABLE %s.%s", pgduckdb_db_and_schema_string(postgres_schema_name, true),
			                            quote_identifier(table_name));
			pgduckdb::DuckDBQueryOrThrow(*connection, drop_query);
		}
	}

	/*
	 * And now we basically do the same thing as above, but for TEMP tables.
	 * For those we need to check our in-memory temporary_duckdb_tables set.
	 */
	ret = SPI_exec(R"(
		SELECT objid, object_name
		FROM pg_catalog.pg_event_trigger_dropped_objects()
		WHERE object_type = 'table'
			AND schema_name = 'pg_temp'
		)",
	               0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

	for (auto proc = 0; proc < SPI_processed; ++proc) {
		HeapTuple tuple = SPI_tuptable->vals[proc];

		bool isnull;
		Datum relid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull) {
			elog(ERROR, "Expected relid to be returned, but found NULL");
		}
		Oid relid = DatumGetObjectId(relid_datum);
		if (temporary_duckdb_tables.count(relid) == 0) {
			/* It was a regular temporary table, so this DROP is allowed */
			continue;
		}
		if (!connection) {
			/*
			 * For now, we don't support DuckDB queries in transactions. To support
			 * write queries in transactions we'll need to link Postgres and DuckdB
			 * their transaction lifecycles.
			 */
			PreventInTransactionBlock(true, "DuckDB queries");
			connection = pgduckdb::DuckDBManager::CreateConnection();
			pgduckdb::DuckDBQueryOrThrow(*connection, "BEGIN TRANSACTION");
		}
		char *table_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);
		pgduckdb::DuckDBQueryOrThrow(*connection,
		                             std::string("DROP TABLE pg_temp.main.") + quote_identifier(table_name));
		temporary_duckdb_tables.erase(relid);
	}

	AtEOXact_GUC(false, save_nestlevel);
	SPI_finish();

	if (connection) {
		pgduckdb::DuckDBQueryOrThrow(*connection, "COMMIT");
	}
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(duckdb_alter_table_trigger);
Datum
duckdb_alter_table_trigger(PG_FUNCTION_ARGS) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	if (!pgduckdb::IsExtensionRegistered()) {
		/*
		 * We're not installed, so don't mess with the query. Normally this
		 * shouldn't happen, but better safe than sorry.
		 */
		PG_RETURN_NULL();
	}

	SPI_connect();

	/*
	 * Temporarily escalate privileges to superuser so we can insert into
	 * duckdb.tables. We temporarily clear the search_path to protect against
	 * search_path confusion attacks. See guidance on SECURITY DEFINER
	 * functions in postgres for details:
	 * https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY
	 */
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "", PGC_USERSET, PGC_S_SESSION);
	Oid saved_userid;
	int sec_context;
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);
	/*
	 * Check if a table was altered that was created using the duckdb access.
	 * This needs to check both pg_class, duckdb.tables, and the
	 * temporary_duckdb_tables set, because the access method might have been
	 * changed from/to duckdb by the ALTER TABLE SET ACCESS METHOD command.
	 */
	int ret = SPI_exec(R"(
		SELECT objid as relid, false AS needs_to_check_temporary_set
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN pg_catalog.pg_class
		ON cmds.objid = pg_class.oid
		WHERE cmds.object_type = 'table'
		AND pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'duckdb')
		UNION ALL
		SELECT objid as relid, false AS needs_to_check_temporary_set
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN duckdb.tables AS ddbtables
		ON cmds.objid = ddbtables.relid
		WHERE cmds.object_type = 'table'
		UNION ALL
		SELECT objid as relid, true AS needs_to_check_temporary_set
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN pg_catalog.pg_class
		ON cmds.objid = pg_class.oid
		WHERE cmds.object_type = 'table'
		AND pg_class.relam != (SELECT oid FROM pg_am WHERE amname = 'duckdb')
		AND pg_class.relpersistence = 't'
		)",
	                   0);

	/* Revert back to original privileges */
	SetUserIdAndSecContext(saved_userid, sec_context);
	AtEOXact_GUC(false, save_nestlevel);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

	/* if we inserted a row it was a duckdb table */
	auto is_possibly_duckdb_table = SPI_processed > 0;
	if (!is_possibly_duckdb_table || pgduckdb::doing_motherduck_sync) {
		/* No DuckDB tables were altered, or we don't want to forward DDL to
		 * DuckDB because we're syncing with MotherDuck */
		SPI_finish();
		PG_RETURN_NULL();
	}

	HeapTuple tuple = SPI_tuptable->vals[0];
	bool isnull;
	Datum relid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull) {
		elog(ERROR, "Expected relid to be returned, but found NULL");
	}
	Datum needs_to_check_temporary_set_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull);
	if (isnull) {
		elog(ERROR, "Expected temporary boolean to be returned, but found NULL");
	}

	Oid relid = DatumGetObjectId(relid_datum);
	bool needs_to_check_temporary_set = DatumGetBool(needs_to_check_temporary_set_datum);
	SPI_finish();

	if (needs_to_check_temporary_set) {
		if (temporary_duckdb_tables.count(relid) == 0) {
			/* It was a regular temporary table, so this ALTER is allowed */
			PG_RETURN_NULL();
		}
	}

	elog(ERROR, "DuckDB does not support ALTER TABLE yet");

	PG_RETURN_NULL();
}

/*
 * This event trigger is called when a GRANT statement is executed. We use it to
 * block GRANTs on DuckDB tables. We allow grants on schemas though.
 */
PG_FUNCTION_INFO_V1(duckdb_grant_trigger);
Datum
duckdb_grant_trigger(PG_FUNCTION_ARGS) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	if (!pgduckdb::IsExtensionRegistered()) {
		/*
		 * We're not installed, so don't mess with the query. Normally this
		 * shouldn't happen, but better safe than sorry.
		 */
		PG_RETURN_NULL();
	}

	EventTriggerData *trigdata = (EventTriggerData *)fcinfo->context;
	Node *parsetree = trigdata->parsetree;
	if (!IsA(parsetree, GrantStmt)) {
		/*
		 * Not a GRANT statement, so don't mess with the query. This is not
		 * expected though.
		 */
		PG_RETURN_NULL();
	}

	GrantStmt *stmt = castNode(GrantStmt, parsetree);
	if (stmt->objtype != OBJECT_TABLE) {
		/* We only care about blocking table grants */
		PG_RETURN_NULL();
	}

	if (stmt->targtype != ACL_TARGET_OBJECT) {
		/*
		 * We only care about blocking exact object grants. We don't want to
		 * block ALL IN SCHEMA or ALTER DEFAULT PRIVELEGES.
		 */
		PG_RETURN_NULL();
	}

	foreach_node(RangeVar, object, stmt->objects) {
		Oid relation_oid = RangeVarGetRelid(object, AccessShareLock, true);
		Relation relation = RelationIdGetRelation(relation_oid);
		if (pgduckdb::IsMotherDuckTable(relation)) {
			elog(ERROR, "MotherDuck tables do not support GRANT");
		}
		RelationClose(relation);
	}

	PG_RETURN_NULL();
}
}
