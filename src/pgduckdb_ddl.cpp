#include "pgduckdb/pgduckdb_planner.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"
#include "pgduckdb/pgduckdb_guc.h"
#include "pgduckdb/pgduckdb_ddl.hpp"
#include "pgduckdb/pgduckdb_hooks.hpp"
#include "pgduckdb/pgduckdb_planner.hpp"

extern "C" {
#include "postgres.h"
#include "access/tableam.h"
#include "access/relation.h"
#include "catalog/pg_type.h"
#include "commands/event_trigger.h"
#include "fmgr.h"
#include "catalog/pg_authid_d.h"
#include "catalog/namespace.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "nodes/print.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "commands/event_trigger.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "rewrite/rewriteHandler.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
#include "pgduckdb/pgduckdb_ruleutils.h"
}

#include "pgduckdb/pgduckdb_guc.h"
#include "pgduckdb/utility/cpp_wrapper.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgduckdb/pgduckdb_fdw.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_userdata_cache.hpp"
#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include <inttypes.h>

namespace pgduckdb {
DDLType top_level_duckdb_ddl_type = DDLType::NONE;
bool refreshing_materialized_view = false;
} // namespace pgduckdb

/*
 * ctas_skip_data stores the original value of the skipData field of the
 * CreateTableAsStmt of the query that's currently being executed. For duckdb
 * tables we force this value to false.
 */
static bool ctas_skip_data = false;

static ProcessUtility_hook_type prev_process_utility_hook = NULL;

/*
 * WrapQueryInQueryCall takes a query and wraps it an duckdb.query(...) call.
 * It then explicitly references all the columns and the types from the
 * original qeury its target list. So a query like this:
 *
 * SELECT r from read_csv('file.csv') r;
 *
 * Would expand to:
 *
 * SELECT r['id']::int AS id, r['name']::text AS name
 * FROM duckdb.query('SELECT * from system.main.read_csv(''file.csv'')') r;
 *
 */
static Query *
WrapQueryInDuckdbQueryCall(Query *query, List *target_list) {
	char *duckdb_query_string = pgduckdb_get_querydef(query);

	StringInfo buf = makeStringInfo();
	appendStringInfo(buf, "SELECT ");
	bool first = true;
	foreach_node(TargetEntry, tle, target_list) {
		if (!first) {
			appendStringInfoString(buf, ", ");
		}

		Oid type = exprType((Node *)tle->expr);
		Oid typemod = exprTypmod((Node *)tle->expr);
		first = false;
		appendStringInfo(buf, "r[%s]::%s AS %s", quote_literal_cstr(tle->resname),
		                 format_type_with_typemod(type, typemod), quote_identifier(tle->resname));
	}

	appendStringInfo(buf, " FROM duckdb.query(%s) r", quote_literal_cstr(duckdb_query_string));

	List *parsetree_list = pg_parse_query(buf->data);

	/*
	 * We only allow a single user statement in a prepared statement. This is
	 * mainly to keep the protocol simple --- otherwise we'd need to worry
	 * about multiple result tupdescs and things like that.
	 */
	if (list_length(parsetree_list) > 1)
		ereport(ERROR,
		        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("BUG: pg_duckdb generated a command with multiple queries")));

#if PG_VERSION_NUM >= 150000
	return parse_analyze_fixedparams((RawStmt *)linitial(parsetree_list), buf->data, NULL, 0, NULL);
#else
	return parse_analyze((RawStmt *)linitial(parsetree_list), buf->data, NULL, 0, NULL);
#endif
}

/*
 * This is a DROP pre-check to check if a DROP TABLE command tries to drop both
 * DuckDB and Postgres tables. Doing so is not allowed within an explicit
 * transaction. It also claims the command ID
 */
static void
DropTablePreCheck(DropStmt *stmt) {
	Assert(stmt->removeType == OBJECT_TABLE);
	bool dropping_duckdb_tables = false;
	bool dropping_postgres_tables = false;
	foreach_node(List, obj, stmt->objects) {
		/* check if table is duckdb table */
		RangeVar *rel = makeRangeVarFromNameList(obj);
		Oid relid = RangeVarGetRelid(rel, AccessShareLock, true);
		if (!OidIsValid(relid)) {
			/* Let postgres deal with this. It's possible that this is fine in
			 * case of DROP ... IF EXISTS */
			continue;
		}

		Relation relation = relation_open(relid, AccessShareLock);
		if (pgduckdb::IsDuckdbTable(relation)) {
			if (!dropping_duckdb_tables) {
				pgduckdb::ClaimCurrentCommandId();
				dropping_duckdb_tables = true;
			}
		} else {
			dropping_postgres_tables = true;
		}
		relation_close(relation, NoLock);
	}

	if (!pgduckdb::MixedWritesAllowed() && dropping_duckdb_tables && dropping_postgres_tables) {
		elog(ERROR, "Dropping both DuckDB and non-DuckDB tables in the same transaction is not supported");
	}
}

static void DuckdbHandleCreateForeignServerStmt(Node *parsetree);
static void DuckdbHandleAlterForeignServerStmt(Node *parsetree);
static void DuckdbHandleCreateUserMappingStmt(Node *parsetree);
static void DuckdbHandleAlterUserMappingStmt(Node *parsetree);

static void
DuckdbHandleDDL(PlannedStmt *pstmt, const char *query_string, ParamListInfo params) {
	if (!pgduckdb::IsExtensionRegistered()) {
		/* We're not installed, so don't mess with the query */
		return;
	}

	Node *parsetree = pstmt->utilityStmt;

	if (IsA(parsetree, TransactionStmt)) {
		/*
		 * Don't do anything for transaction statements, we have the
		 * transaction XactCallback functions to handle those in
		 * pgduckdb_xact.cpp
		 */
		return;
	}

	/*
	 * Time to check for disallowed mixed writes here. The handling for some of
	 * the DuckDB DDL, marks some some specific mixed writes as allowed. If we
	 * don't check now, we might accidentally also mark disallowed mixed writes
	 * from before as allowed.
	 */

	if (IsA(parsetree, CreateStmt)) {
		auto stmt = castNode(CreateStmt, parsetree);

		/* Default to duckdb AM for ddb$ schemas and disallow other AMs */
		Oid schema_oid = RangeVarGetCreationNamespace(stmt->relation);
		char *schema_name = get_namespace_name(schema_oid);
		if (strncmp("ddb$", schema_name, 4) == 0) {
			if (!stmt->accessMethod) {
				stmt->accessMethod = pstrdup("duckdb");
			} else if (strcmp(stmt->accessMethod, "duckdb") != 0) {
				ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				                errmsg("Creating a non-DuckDB table in a ddb$ schema is not supported")));
			}
		}

		char *access_method = stmt->accessMethod ? stmt->accessMethod : default_table_access_method;
		bool is_duckdb_table = strcmp(access_method, "duckdb") == 0;
		if (is_duckdb_table) {
			if (pgduckdb::top_level_duckdb_ddl_type != pgduckdb::DDLType::NONE) {
				ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				                errmsg("Only one DuckDB table can be created in a single statement")));
			}
			pgduckdb::top_level_duckdb_ddl_type = pgduckdb::DDLType::CREATE_TABLE;
			pgduckdb::ClaimCurrentCommandId();
		}

		return;
	} else if (IsA(parsetree, CreateTableAsStmt)) {
		auto stmt = castNode(CreateTableAsStmt, parsetree);

		/* Default to duckdb AM for ddb$ schemas and disallow other AMs */
		Oid schema_oid = RangeVarGetCreationNamespace(stmt->into->rel);
		char *schema_name = get_namespace_name(schema_oid);
		if (strncmp("ddb$", schema_name, 4) == 0) {
			if (!stmt->into->accessMethod) {
				stmt->into->accessMethod = pstrdup("duckdb");
			} else if (strcmp(stmt->into->accessMethod, "duckdb") != 0) {
				ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				                errmsg("Creating a non-DuckDB table in a ddb$ schema is not supported")));
			}
		}
		char *access_method = stmt->into->accessMethod ? stmt->into->accessMethod : default_table_access_method;
		bool is_duckdb_table = strcmp(access_method, "duckdb") == 0;
		if (is_duckdb_table) {
			if (pgduckdb::top_level_duckdb_ddl_type != pgduckdb::DDLType::NONE) {
				ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				                errmsg("Only one DuckDB table can be created in a single statement")));
			}
			pgduckdb::top_level_duckdb_ddl_type = pgduckdb::DDLType::CREATE_TABLE;
			pgduckdb::ClaimCurrentCommandId();
			/*
			 * Force skipData to false for duckdb tables, so that Postgres does
			 * not execute the query, and save the original value in ctas_skip_data
			 * so we can use it later in duckdb_create_table_trigger to choose
			 * whether to execute the query in DuckDB or not.
			 */
			ctas_skip_data = stmt->into->skipData;
			stmt->into->skipData = true;
		}

		bool is_create_materialized_view = stmt->into->viewQuery != NULL;
		bool skips_planning = stmt->into->skipData || is_create_materialized_view;
		if (!skips_planning) {
			// No need to plan here as well, because standard_ProcessUtility
			// will already plan the query and thus get the correct columns and
			// their types.
			return;
		}

		Query *original_query = castNode(Query, stmt->query);

		// For cases where Postgres does not usually plan the query, sometimes
		// we still need to. Specifically for these cases:
		// 1. If we're creating a DuckDB table, we need to plan the query
		//    because the types that Postgres inferred might be different than
		//    the ones that DuckDB execution inferred, e.g. when reading a
		//    JSONB column from a Postgres table it will result in a JSONB
		//    column according to Postgres, but DuckDB execution will actually
		//    return a JSON column.
		// 2. Similarly, if the query will use DuckDB execution (either because
		//    it's required or because duckdb.force_execution is set to true),
		//    we need to plan the query so that we can get the correct types
		//    for the target list. This is because DuckDB execution will not be
		//    able to infer the types from the target list, and will instead
		//    use the types that Postgres inferred.
		//
		// One final important thing to consider is that for materialized views
		// it's really bad if a future REFRESH MATERIALIZED VIEW command
		// returns different column types than the types that were returned by
		// the query during creation. That can easily result in data
		// corruption. To make sure this doesn't happen we do a few things:
		// - Wrap the resulting duckdb query in a duckdb.query() call, this
		//   means that even if the query did not require motherduck, it will
		//   still use duckdb execution in any future refresh calls.
		// - For the reverse problem, we make sure that REFRESH MATARIALIZED
		//   VIEW ignores duckdb.force_execution (see the code comments in
		//   ShouldTryToUseDuckdbExecution for details)
		// - Explicity select all the arguments from the target list, from the
		//   duckdb.query() call and cast them to the expected type. This way,
		//   a "SELECT * FROM read_csv(...)" will always return the same
		//   columns and column types, even if the CSV is changed.
		bool needs_planning = is_duckdb_table || pgduckdb::NeedsDuckdbExecution(original_query) ||
		                      pgduckdb::ShouldTryToUseDuckdbExecution(original_query);

		if (!needs_planning) {
			// If we don't need planning let's not do anything though.
			return;
		}

		/* NOTE: The below code is mostly copied from ExecCreateTableAs */
		Query *query = (Query *)copyObjectImpl(original_query);
		/*
		 * Parse analysis was done already, but we still have to run the rule
		 * rewriter.  We do not do AcquireRewriteLocks: we assume the query
		 * either came straight from the parser, or suitable locks were
		 * acquired by plancache.c.
		 */
		List *rewritten = QueryRewrite(query);

		/* SELECT should never rewrite to more or less than one SELECT query */
		if (list_length(rewritten) != 1)
			elog(ERROR, "unexpected rewrite result for CREATE TABLE AS SELECT, contains %d queries",
			     list_length(rewritten));
		query = linitial_node(Query, rewritten);
		Assert(query->commandType == CMD_SELECT);

		Query *rewritten_query_copy = (Query *)copyObjectImpl(query);

		/*
		 * We call into our DuckDB planning logic directly here, to ensure that
		 * this query is planned using DuckDB. This is needed for a CTAS into a
		 * DuckDB table, where the query itself does not require DuckDB
		 * execution. If we don't set force_execution to true, the query will
		 * be planned using Postgres, but then later executed using DuckDB when
		 * we actually write into the DuckDB table (possibly resulting in
		 * different column types).
		 */
		pgduckdb::IsAllowedStatement(query, true);
		PlannedStmt *plan = DuckdbPlanNode(query, query_string, CURSOR_OPT_PARALLEL_OK, params, true);

		/* This is where our custom code starts again */
		List *target_list = plan->planTree->targetlist;

		stmt->query = (Node *)WrapQueryInDuckdbQueryCall(rewritten_query_copy, target_list);

		if (is_create_materialized_view) {
			/*
			 * If this is a materialized view we also need to update its view
			 * query. It's important not to set it for regular CTAS queries,
			 * otherwise Postgres code will assume it's a materialized view
			 * instead.
			 */
			stmt->into->viewQuery = (Node *)copyObjectImpl(stmt->query);
		}
	} else if (IsA(parsetree, RefreshMatViewStmt)) {
		/* We need to set the top level DDL type to REFRESH_MATERIALIZED_VIEW
		 * here, because want ignore the value of duckdb.force_execution for
		 * the duration of this REFRESH. See the code comment in
		 * ShouldTryToUseDuckdbExecution for details. */
		pgduckdb::top_level_duckdb_ddl_type = pgduckdb::DDLType::REFRESH_MATERIALIZED_VIEW;
	} else if (IsA(parsetree, CreateSchemaStmt) && !pgduckdb::doing_motherduck_sync) {
		auto stmt = castNode(CreateSchemaStmt, parsetree);
		if (stmt->schemaname) {
			if (strncmp("ddb$", stmt->schemaname, 4) == 0) {
				elog(ERROR, "Creating ddb$ schemas is currently not supported");
			}
		} else if (stmt->authrole && stmt->authrole->roletype == ROLESPEC_CSTRING) {
			if (strncmp("ddb$", stmt->authrole->rolename, 4) == 0) {
				elog(ERROR, "Creating ddb$ schemas is currently not supported");
			}
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
	} else if (IsA(parsetree, DropStmt)) {
		auto stmt = castNode(DropStmt, parsetree);
		switch (stmt->removeType) {
		case OBJECT_TABLE:
			DropTablePreCheck(stmt);
			break;
		default:
			/* ignore anything else */
			break;
		}
		return;
	} else if (IsA(parsetree, AlterTableStmt)) {
		auto stmt = castNode(AlterTableStmt, parsetree);
		Oid relation_oid = RangeVarGetRelid(stmt->relation, AccessShareLock, false);
		Relation relation = RelationIdGetRelation(relation_oid);
		/*
		 * Certain CREATE TABLE commands also trigger an ALTER TABLE command,
		 * specifically if you use REFERENCES it will alter the table
		 * afterwards. We currently only do this to get a better error message,
		 * because we don't support REFERENCES anyway.
		 */
		if (pgduckdb::IsDuckdbTable(relation) && pgduckdb::top_level_duckdb_ddl_type == pgduckdb::DDLType::NONE) {
			pgduckdb::top_level_duckdb_ddl_type = pgduckdb::DDLType::ALTER_TABLE;
			pgduckdb::ClaimCurrentCommandId();
		}
		RelationClose(relation);
	} else if (IsA(parsetree, CreateForeignServerStmt)) {
		DuckdbHandleCreateForeignServerStmt(parsetree);
		return;
	} else if (IsA(parsetree, AlterForeignServerStmt)) {
		DuckdbHandleAlterForeignServerStmt(parsetree);
		return;
	} else if (IsA(parsetree, CreateUserMappingStmt)) {
		DuckdbHandleCreateUserMappingStmt(parsetree);
		return;
	} else if (IsA(parsetree, AlterUserMappingStmt)) {
		DuckdbHandleAlterUserMappingStmt(parsetree);
		return;
	}
}

static void
DuckdbHandleAlterForeignServerStmt(Node *parsetree) {
	pgduckdb::CurrentServerType = nullptr;
	// Propagate the "TYPE" to the server options, don't validate it here though
	auto stmt = castNode(AlterForeignServerStmt, parsetree);
	auto server = GetForeignServerByName(stmt->servername, false);

	auto duckdb_fdw_oid = get_foreign_data_wrapper_oid("duckdb", false);
	if (server->fdwid != duckdb_fdw_oid) {
		return; // Not our FDW, don't do anything
	}

	if (server->servertype == NULL) {
		return; // Don't do anything quite yet, let the validator raise an error
	}

	pgduckdb::CurrentServerType = pstrdup(server->servertype);
}

static void
DuckdbHandleCreateForeignServerStmt(Node *parsetree) {
	pgduckdb::CurrentServerType = nullptr;
	// Propagate the "TYPE" to the server options, don't validate it here though
	auto stmt = castNode(CreateForeignServerStmt, parsetree);
	if (strcmp(stmt->fdwname, "duckdb") != 0) {
		return; // Not our FDW, don't do anything
	}

	if (stmt->servertype == NULL) {
		return; // Don't do anything quite yet, let the validator raise an error
	}

	pgduckdb::CurrentServerType = pstrdup(stmt->servertype);
}

static void
DuckdbHandleAlterUserMappingStmt(Node *parsetree) {
	pgduckdb::CurrentServerOid = InvalidOid;
	// Propagate the "servername" to the user mapping options
	auto stmt = castNode(AlterUserMappingStmt, parsetree);

	auto server = GetForeignServerByName(stmt->servername, false);
	auto duckdb_fdw_oid = get_foreign_data_wrapper_oid("duckdb", false);
	if (server->fdwid != duckdb_fdw_oid) {
		return; // Not our FDW, don't do anything
	}

	pgduckdb::CurrentServerOid = server->serverid;
}

static void
DuckdbHandleCreateUserMappingStmt(Node *parsetree) {
	pgduckdb::CurrentServerOid = InvalidOid;
	// Propagate the "servername" to the user mapping options
	auto stmt = castNode(CreateUserMappingStmt, parsetree);

	auto server = GetForeignServerByName(stmt->servername, false);
	auto fdw = GetForeignDataWrapper(server->fdwid);
	if (strcmp(fdw->fdwname, "duckdb") != 0) {
		return; // Not our FDW, don't do anything
	}

	pgduckdb::CurrentServerOid = server->serverid;
}

static void
DuckdbUtilityHook_Cpp(PlannedStmt *pstmt, const char *query_string, bool read_only_tree, ProcessUtilityContext context,
                      ParamListInfo params, struct QueryEnvironment *query_env, DestReceiver *dest,
                      QueryCompletion *qc) {
	Node *parsetree = pstmt->utilityStmt;
	if (IsA(parsetree, CopyStmt)) {
		auto copy_query = PostgresFunctionGuard(MakeDuckdbCopyQuery, pstmt, query_string, query_env);
		if (copy_query) {
			auto res = pgduckdb::DuckDBQueryOrThrow(copy_query);
			auto chunk = res->Fetch();
			auto processed = chunk->GetValue(0, 0).GetValue<uint64_t>();
			if (qc) {
				SetQueryCompletion(qc, CMDTAG_COPY, processed);
			}
			return;
		}
	}

	/*
	 * We need this prev_top_level_ddl variable because its possible that the
	 * first DDL command then triggers a second DDL command. The first of which
	 * is at the top level, but the second of which is not. So this way we make
	 * sure that the our top level state is the correct one for the current
	 * command.
	 *
	 * NOTE: We don't care about resetting the global variable in case of an
	 * error, because we'll set it correctly for the next command anyway.
	 */
	bool prev_top_level_ddl = pgduckdb::IsStatementTopLevel();
	pgduckdb::SetStatementTopLevel(context == PROCESS_UTILITY_TOPLEVEL);

	DuckdbHandleDDL(pstmt, query_string, params);
	prev_process_utility_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);

	pgduckdb::SetStatementTopLevel(prev_top_level_ddl);
}

static void
DuckdbUtilityHook(PlannedStmt *pstmt, const char *query_string, bool read_only_tree, ProcessUtilityContext context,
                  ParamListInfo params, struct QueryEnvironment *query_env, DestReceiver *dest, QueryCompletion *qc) {
	if (!pgduckdb::IsExtensionRegistered()) {
		return prev_process_utility_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
	}

	InvokeCPPFunc(DuckdbUtilityHook_Cpp, pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
}

void
DuckdbInitUtilityHook() {
	prev_process_utility_hook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = DuckdbUtilityHook;
}

/*
 * Truncates the given table in DuckDB.
 */
void
DuckdbTruncateTable(Oid relation_oid) {
	auto name = PostgresFunctionGuard(pgduckdb_relation_name, relation_oid);
	pgduckdb::DuckDBQueryOrThrow(std::string("TRUNCATE ") + name);
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
		break;
	}
}

extern "C" {

DECLARE_PG_FUNCTION(duckdb_create_table_trigger) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	if (!pgduckdb::IsExtensionRegistered()) {
		/*
		 * We're not installed, so don't mess with the query. Normally this
		 * shouldn't happen, but better safe than sorry.
		 */
		PG_RETURN_NULL();
	}

	if (pgduckdb::top_level_duckdb_ddl_type != pgduckdb::DDLType::CREATE_TABLE &&
	    pgduckdb::top_level_duckdb_ddl_type != pgduckdb::DDLType::NONE) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
		                errmsg("Cannot create a DuckDB table this way, use CREATE TABLE or CREATE TABLE ... AS")));
		PG_RETURN_NULL();
	}
	/* Reset it back to NONE, for the remainder of the event trigger */
	pgduckdb::top_level_duckdb_ddl_type = pgduckdb::DDLType::NONE;

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
	 * We also temporarily force duckdb.force_execution to false, because
	 * pg_catalog.pg_event_trigger_ddl_commands does not exist in DuckDB.
	 */
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

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
		elog(ERROR, "Expected single table to be created, but found %" PRIu64, static_cast<uint64_t>(SPI_processed));
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
		pgduckdb::RegisterDuckdbTempTable(relid);
	} else {
		if (!pgduckdb::IsMotherDuckEnabled()) {
			elog(ERROR, "Only TEMP tables are supported in DuckDB if MotherDuck support is not enabled");
		}

		Oid saved_userid;
		int sec_context;
		const char *postgres_schema_name = get_namespace_name_or_temp(get_rel_namespace(relid));
		const char *duckdb_db = (const char *)linitial(pgduckdb_db_and_schema(postgres_schema_name, true));
		auto default_db = pgduckdb::DuckDBManager::Get().GetDefaultDBName();

		Oid arg_types[] = {OIDOID, TEXTOID, TEXTOID, TEXTOID};
		Datum values[] = {relid_datum, CStringGetTextDatum(duckdb_db), 0, CStringGetTextDatum(default_db.c_str())};
		char nulls[] = {' ', ' ', 'n', ' '};

		if (pgduckdb::doing_motherduck_sync) {
			values[2] = CStringGetTextDatum(pgduckdb::current_motherduck_catalog_version);
			nulls[2] = ' ';
		}

		GetUserIdAndSecContext(&saved_userid, &sec_context);
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);
		pgduckdb::pg::SetForceAllowWrites(true);
		ret = SPI_execute_with_args(R"(
			INSERT INTO duckdb.tables (relid, duckdb_db, motherduck_catalog_version, default_database)
			VALUES ($1, $2, $3, $4)
			)",
		                            lengthof(arg_types), arg_types, values, nulls, false, 0);
		pgduckdb::pg::SetForceAllowWrites(false);

		/* Revert back to original privileges */
		SetUserIdAndSecContext(saved_userid, sec_context);

		if (ret != SPI_OK_INSERT) {
			elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
		}

		ATExecChangeOwner(relid, pgduckdb::MotherDuckPostgresUser(), false, AccessExclusiveLock);
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

	pgduckdb::ClaimCurrentCommandId(true);

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

	/* We're going to run multiple queries in DuckDB, so we need to start a
	 * transaction to ensure ACID guarantees hold. */
	auto connection = pgduckdb::DuckDBManager::GetConnection(true);
	Query *ctas_query = nullptr;

	if (IsA(parsetree, CreateTableAsStmt) && !ctas_skip_data) {
		auto stmt = castNode(CreateTableAsStmt, parsetree);
		ctas_query = (Query *)stmt->query;
	}

	pgduckdb::DuckDBQueryOrThrow(*connection, create_table_string);
	if (ctas_query) {
		const char *ctas_query_string = pgduckdb_get_querydef(ctas_query);

		std::string insert_string =
		    std::string("INSERT INTO ") + pgduckdb_relation_name(relid) + " " + ctas_query_string;
		pgduckdb::DuckDBQueryOrThrow(*connection, insert_string);
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
DECLARE_PG_FUNCTION(duckdb_drop_trigger) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	if (!pgduckdb::IsExtensionRegistered()) {
		/*
		 * We're not installed, so don't mess with the query. Normally this
		 * shouldn't happen, but better safe than sorry.
		 */
		PG_RETURN_NULL();
	}

	/*
	 * Save the current top level state, because our next SPI commands will
	 * cause it to always become true.
	 */
	bool is_top_level = pgduckdb::IsStatementTopLevel();

	SPI_connect();

	/*
	 * Temporarily escalate privileges to superuser so we can insert into
	 * duckdb.tables. We temporarily clear the search_path to protect against
	 * search_path confusion attacks. See guidance on SECURITY DEFINER
	 * functions in postgres for details:
	 * https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY
	 *
	 * We also temporarily force duckdb.force_execution to false, because
	 * pg_catalog.pg_event_trigger_dropped_objects does not exist in DuckDB.
	 */
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

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

	/*
	 * We don't want to allow DROPs that involve PG objects and DuckDB tables
	 * in the same transaction. This is not easy to disallow though, because
	 * when dropping a DuckDB table there are also many other objects that get dropped:
	 * 1. Each table owns two types:
	 *	 a. the composite type matching its columns
	 *	 b. the array of that composite type
	 * 2. There can also be many implicitely connected things to a table, like sequences/constraints/etc
	 *
	 * So here we try to count all the objects that are not connected to a
	 * table. Sadly at this stage the objects are already deleted, so there's
	 * no way to know for sure if they were. So we use the fairly crude
	 * approach simply not counting all the object types that are often
	 * connected to tables. We might have missed a few, so we can start to
	 * ignore more if we get bug reports. We also might want to change how wo
	 * do this completely, but for now this seems to work well enough.
	 *
	 * One thing that we at least want to disallow (for now) is removing a
	 * non-"ddb$" schema and all its DuckDB tables. That way if there are also
	 * other objects in this schema, we automatically fail the drop. This is
	 * also up for debate in the future, but it's much easier to decide that we
	 * want to start allowing this than it is to start disallowing it.
	 */
	int ret = SPI_exec(R"(
		SELECT count(*)::bigint, (count(*) FILTER (WHERE object_type = 'table'))::bigint
		FROM pg_catalog.pg_event_trigger_dropped_objects()
		WHERE object_type NOT IN ('type', 'sequence', 'table constraint', 'index', 'default value', 'trigger', 'toast table')
	)",
	                   0);
	if (ret != SPI_OK_SELECT) {
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
	}

	if (SPI_processed != 1) {
		elog(ERROR, "expected a single row to be returned, but found %" PRIu64, static_cast<uint64_t>(SPI_processed));
	}

	int64 total_deleted;
	int64 deleted_tables;

	{
		HeapTuple tuple = SPI_tuptable->vals[0];
		bool isnull;
		Datum total_deleted_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull) {
			elog(ERROR, "Expected number of deleted objects but found NULL");
		}
		total_deleted = DatumGetInt64(total_deleted_datum);

		Datum deleted_tables_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull);
		if (isnull) {
			elog(ERROR, "Expected number of deleted tables but found NULL");
		}
		deleted_tables = DatumGetInt64(deleted_tables_datum);
	}

	if (deleted_tables == 0) {
		/*
		 * No tables were deleted at all, thus also no duckdb tables. So no
		 * need to do anything.
		 */
		AtEOXact_GUC(false, save_nestlevel);
		SPI_finish();
		PG_RETURN_NULL();
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

	pgduckdb::pg::SetForceAllowWrites(true);
	ret = SPI_exec(R"(
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
	pgduckdb::pg::SetForceAllowWrites(false);

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
	duckdb::Connection *connection = nullptr;

	int64 deleted_duckdb_tables = 0;

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
		for (uint64_t proc = 0; proc < SPI_processed; ++proc) {
			if (!connection) {
				/* We're going to run multiple queries in DuckDB, so we need to
				 * start a transaction to ensure ACID guarantees hold. */
				connection = pgduckdb::DuckDBManager::GetConnection(true);
			}
			HeapTuple tuple = SPI_tuptable->vals[proc];

			char *postgres_schema_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
			char *table_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);
			char *drop_query = psprintf("DROP TABLE %s.%s", pgduckdb_db_and_schema_string(postgres_schema_name, true),
			                            quote_identifier(table_name));
			pgduckdb::DuckDBQueryOrThrow(*connection, drop_query);

			deleted_duckdb_tables++;
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

	for (uint64_t proc = 0; proc < SPI_processed; ++proc) {
		HeapTuple tuple = SPI_tuptable->vals[proc];

		bool isnull;
		Datum relid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull) {
			elog(ERROR, "Expected relid to be returned, but found NULL");
		}
		Oid relid = DatumGetObjectId(relid_datum);
		if (!pgduckdb::IsDuckdbTempTable(relid)) {
			/* It was a regular temporary table, so this DROP is allowed */
			continue;
		}

		if (!connection) {
			/* We're going to run multiple queries in DuckDB, so we need to
			 * start a transaction to ensure ACID guarantees hold. */
			connection = pgduckdb::DuckDBManager::GetConnection(true);
		}
		char *table_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);
		pgduckdb::DuckDBQueryOrThrow(*connection,
		                             std::string("DROP TABLE pg_temp.main.") + quote_identifier(table_name));
		pgduckdb::UnregisterDuckdbTempTable(relid);
		deleted_duckdb_tables++;
	}

	/*
	 * Restore "top level" state, because by running SPI commands it's now
	 * always set to false.
	 */
	pgduckdb::SetStatementTopLevel(is_top_level);

	if (!pgduckdb::MixedWritesAllowed() && deleted_duckdb_tables > 0) {
		if (deleted_duckdb_tables < deleted_tables) {
			elog(ERROR, "Dropping both DuckDB and non-DuckDB tables in the same transaction is not supported");
		}

		if (deleted_duckdb_tables < total_deleted) {
			elog(ERROR, "Dropping both DuckDB tables and non-DuckDB objects in the same transaction is not supported");
		}
	}

	if (deleted_duckdb_tables > 0) {
		pgduckdb::ClaimCurrentCommandId(true);
	}

	AtEOXact_GUC(false, save_nestlevel);
	SPI_finish();

	PG_RETURN_NULL();
}

DECLARE_PG_FUNCTION(duckdb_alter_table_trigger) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	if (!pgduckdb::IsExtensionRegistered()) {
		/*
		 * We're not installed, so don't mess with the query. Normally this
		 * shouldn't happen, but better safe than sorry.
		 */
		PG_RETURN_NULL();
	}

	/* Reset since we don't need it anymore */
	if (pgduckdb::top_level_duckdb_ddl_type != pgduckdb::DDLType::ALTER_TABLE &&
	    pgduckdb::top_level_duckdb_ddl_type != pgduckdb::DDLType::NONE) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
		                errmsg("Cannot ALTER a DuckDB table this way, please use ALTER TABLE")));
		PG_RETURN_NULL();
	}
	/* Reset it back to NONE, for the remainder of the event trigger */
	pgduckdb::top_level_duckdb_ddl_type = pgduckdb::DDLType::NONE;

	SPI_connect();

	/*
	 * Temporarily escalate privileges to superuser so we can insert into
	 * duckdb.tables. We temporarily clear the search_path to protect against
	 * search_path confusion attacks. See guidance on SECURITY DEFINER
	 * functions in postgres for details:
	 * https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY
	 *
	 * We also temporarily force duckdb.force_execution to false, because
	 * pg_catalog.pg_event_trigger_dropped_objects does not exist in DuckDB.
	 */
	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);
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
		WHERE cmds.object_type in ('table', 'table column')
		AND pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'duckdb')
		UNION ALL
		SELECT objid as relid, false AS needs_to_check_temporary_set
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN duckdb.tables AS ddbtables
		ON cmds.objid = ddbtables.relid
		WHERE cmds.object_type in ('table', 'table column')
		UNION ALL
		SELECT objid as relid, true AS needs_to_check_temporary_set
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN pg_catalog.pg_class
		ON cmds.objid = pg_class.oid
		WHERE cmds.object_type in ('table', 'table column')
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
		if (!pgduckdb::IsDuckdbTempTable(relid)) {
			/* It was a regular temporary table, so this ALTER is allowed */
			PG_RETURN_NULL();
		}
	}

	/* Forcibly allow whatever writes Postgres did for this command */
	pgduckdb::ClaimCurrentCommandId(true);

	/* We're going to run multiple queries in DuckDB, so we need to start a
	 * transaction to ensure ACID guarantees hold. */
	auto connection = pgduckdb::DuckDBManager::GetConnection(true);

	EventTriggerData *trigdata = (EventTriggerData *)fcinfo->context;
	char *alter_table_stmt_string;
	if (IsA(trigdata->parsetree, AlterTableStmt)) {
		AlterTableStmt *alter_table_stmt = (AlterTableStmt *)trigdata->parsetree;
		alter_table_stmt_string = pgduckdb_get_alter_tabledef(relid, alter_table_stmt);
	} else if (IsA(trigdata->parsetree, RenameStmt)) {
		RenameStmt *rename_stmt = (RenameStmt *)trigdata->parsetree;
		alter_table_stmt_string = pgduckdb_get_rename_tabledef(relid, rename_stmt);
	} else {
		elog(ERROR, "Unexpected parsetree type: %d", nodeTag(trigdata->parsetree));
	}

	elog(DEBUG1, "Executing: %s", alter_table_stmt_string);
	auto res = pgduckdb::DuckDBQueryOrThrow(*connection, alter_table_stmt_string);

	PG_RETURN_NULL();
}

/*
 * This event trigger is called when a GRANT statement is executed. We use it to
 * block GRANTs on DuckDB tables. We allow grants on schemas though.
 */
DECLARE_PG_FUNCTION(duckdb_grant_trigger) {
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
		Oid relation_oid = RangeVarGetRelid(object, AccessShareLock, false);
		Relation relation = RelationIdGetRelation(relation_oid);
		if (pgduckdb::IsMotherDuckTable(relation)) {
			elog(ERROR, "MotherDuck tables do not support GRANT");
		}
		RelationClose(relation);
	}

	PG_RETURN_NULL();
}
}
