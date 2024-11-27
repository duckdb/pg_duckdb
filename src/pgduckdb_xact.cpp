#include "duckdb/common/exception.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/pg/transactions.hpp"

namespace pgduckdb {

static int64_t duckdb_command_id = -1;
static bool top_level_statement = true;

namespace pg {

/*
 * Returns if we're currently in a transaction block. To determine if we are in
 * a function or not, this uses the tracked top_level_statement variable.
 */
bool
IsInTransactionBlock() {
	return IsInTransactionBlock(top_level_statement);
}

/*
 * Throws an error if we're in a transaction block. To determine if we are in
 * a function or not, this uses the tracked top_level_statement variable.
 */
void
PreventInTransactionBlock(const char *statement_type) {
	PreventInTransactionBlock(top_level_statement, statement_type);
}

/*
 * Check if Postgres did any writes at the end of a transaction.
 *
 * We do this by both checking if there were any WAL writes and as an added
 * measure if the current command id was incremented more than once after the
 * last known DuckDB command.
 *
 * IMPORTANT: This function should only be called at trasaction commit. At
 * other points in the transaction lifecycle its return value is not reliable.
 */
static bool
DidWritesAtTransactionEnd() {
	return pg::DidWalWrites() || pg::GetCurrentCommandId() > duckdb_command_id + 1;
}

} // namespace pg

/*
 * Claim the current command id as being executed by a DuckDB write query.
 *
 * Postgres increments its command id counter for every write query that
 * happens in a transaction. We use this counter to detect if the transaction
 * wrote to both Postgres and DuckDB within the same transaction. The way we do
 * this is by tracking which command id was used for the last DuckDB write. If
 * that difference is more than 1, we know that a Postgres write happened in
 * the middle.
 */
void
ClaimCurrentCommandId() {
	/*
	 * For INSERT/UPDATE/DELETE statements Postgres will already mark the
	 * command counter as used, but not for writes that occur within a PG
	 * select statement. For those cases we mark use the current command ID, if
	 * this is the first write query that we do to DuckDB. Incrementing the
	 * value for every DuckDB write query isn't necessary because we don't use
	 * the value except for checking for cross-database writes. The first
	 * command ID we do want to consume though, otherwise the next Postgres
	 * write query won't increment it, which would make us not detect
	 * cross-database write.
	 */
	bool used = duckdb_command_id == -1;
	CommandId new_command_id = pg::GetCurrentCommandId(used);

	if (new_command_id == duckdb_command_id) {
		return;
	}

	if (!pg::IsInTransactionBlock()) {
		/*
		 * Allow mixed writes outside of a transaction block, this is needed
		 * for DDL.
		 */
		duckdb_command_id = new_command_id;
		return;
	}

	if (new_command_id != duckdb_command_id + 1) {
		throw duckdb::NotImplementedException(
		    "Writing to DuckDB and Postgres tables in the same transaction block is not supported");
	}

	duckdb_command_id = new_command_id;
}

/*
 * Mark the current statement as not being a top level statement.
 *
 * This is used to track if a DuckDB query is executed within a Postgres
 * function. If it is, we don't want to autocommit the query, because the
 * function implicitly runs in a transaction.
 *
 * Sadly there's no easy way to request from Postgres whether we're in a top
 * level statement or not. So we have to track this ourselves.
 */
void
MarkStatementNotTopLevel() {
	top_level_statement = false;
}

/*
 * Trigger Postgres to autocommit single statement queries.
 *
 * We use this as an optimization to avoid the overhead of starting and
 * committing a DuckDB transaction for cases where the user runs only a single
 * query.
 */
void
AutocommitSingleStatementQueries() {
	if (pg::IsInTransactionBlock()) {
		/* We're in a transaction block, we can just execute the query */
		return;
	}

	pg::PreventInTransactionBlock(top_level_statement,
	                              "BUG: You should never see this error we checked IsInTransactionBlock before.");
}

static void
DuckdbXactCallback_Cpp(XactEvent event) {
	/*
	 * We're in a committing phase, always reset the top_level_statement flag,
	 * even if this was not a DuckDB transaction.
	 */
	top_level_statement = true;

	/* If DuckDB is not initialized there's no need to do anything */
	if (!DuckDBManager::IsInitialized()) {
		return;
	}

	auto connection = DuckDBManager::GetConnectionUnsafe();
	auto &context = *connection->context;
	if (!context.transaction.HasActiveTransaction()) {
		duckdb_command_id = -1;
		return;
	}

	switch (event) {
	case XACT_EVENT_PRE_COMMIT:
	case XACT_EVENT_PARALLEL_PRE_COMMIT:
		if (pg::IsInTransactionBlock(top_level_statement)) {
			if (pg::DidWritesAtTransactionEnd() && ddb::DidWrites(context)) {
				throw duckdb::NotImplementedException(
				    "Writing to DuckDB and Postgres tables in the same transaction block is not supported");
			}
		}
		top_level_statement = true;
		duckdb_command_id = -1;
		// Commit the DuckDB transaction too
		context.transaction.Commit();
		break;

	case XACT_EVENT_ABORT:
	case XACT_EVENT_PARALLEL_ABORT:
		top_level_statement = true;
		duckdb_command_id = -1;
		// Abort the DuckDB transaction too
		context.transaction.Rollback(nullptr);
		break;

	case XACT_EVENT_PREPARE:
	case XACT_EVENT_PRE_PREPARE:
		// Throw an error for prepare events. We don't support COMMIT PREPARED.
		throw duckdb::NotImplementedException("Prepared transactions are not implemented in DuckDB.");

	case XACT_EVENT_COMMIT:
	case XACT_EVENT_PARALLEL_COMMIT:
		// No action needed for commit event, we already did committed the
		// DuckDB transaction in the PRE_COMMIT event. We don't commit the
		// DuckDB transaction here, because any failure to commit would
		// then turn into a Postgres PANIC (i.e. a crash). To quote the
		// relevant Postgres comment:
		// > Note that if an error is raised here, it's too late to abort
		// > the transaction. This should be just noncritical resource
		// > releasing.
		break;

	default:
		// Fail hard if future PG versions introduce a new event
		throw duckdb::NotImplementedException("Not implemented XactEvent: %d", event);
	}
}

static void
DuckdbXactCallback(XactEvent event, void * /*arg*/) {
	InvokeCPPFunc(DuckdbXactCallback_Cpp, event);
}

/*
 * Throws an error when starting a new subtransaction in a DuckDB transaction.
 * Existing subtransactions are handled at creation of the DuckDB connection.
 * Throwing here for every event type is problematic, because that would also
 * cause a failure in the resulting sovepoint abort event. Which in turn would
 * cause the postgres error stack to overflow.
 */
static void
DuckdbSubXactCallback_Cpp(SubXactEvent event) {
	if (!DuckDBManager::IsInitialized()) {
		return;
	}
	auto connection = DuckDBManager::GetConnectionUnsafe();
	auto &context = *connection->context;
	if (!context.transaction.HasActiveTransaction()) {
		return;
	}

	if (event == SUBXACT_EVENT_START_SUB) {
		throw duckdb::NotImplementedException("SAVEPOINT is not supported in DuckDB");
	}
}

static void
DuckdbSubXactCallback(SubXactEvent event, SubTransactionId /*my_subid*/, SubTransactionId /*parent_subid*/,
                      void * /*arg*/) {
	InvokeCPPFunc(DuckdbSubXactCallback_Cpp, event);
}

static bool transaction_handler_configured = false;
void
RegisterDuckdbXactCallback() {
	if (transaction_handler_configured) {
		return;
	}
	pg::RegisterXactCallback(DuckdbXactCallback, nullptr);
	pg::RegisterSubXactCallback(DuckdbSubXactCallback, nullptr);
	transaction_handler_configured = true;
}
} // namespace pgduckdb
