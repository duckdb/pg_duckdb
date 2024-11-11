#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "access/xact.h" // RegisterXactCallback and XactEvent
}

namespace pgduckdb {

static void
DuckdbXactCallback_Cpp(XactEvent event, void *arg) {
	if (!started_duckdb_transaction) {
		return;
	}
	auto connection = DuckDBManager::GetConnectionUnsafe();
	auto &context = *connection->context;

	switch (event) {
	case XACT_EVENT_PRE_COMMIT:
	case XACT_EVENT_PARALLEL_PRE_COMMIT:
		// Commit the DuckDB transaction too
		context.transaction.Commit();
		started_duckdb_transaction = false;
		break;

	case XACT_EVENT_ABORT:
	case XACT_EVENT_PARALLEL_ABORT:
		// Abort the DuckDB transaction too
		context.transaction.Rollback(nullptr);
		started_duckdb_transaction = false;
		break;

	case XACT_EVENT_PREPARE:
	case XACT_EVENT_PRE_PREPARE:
		// Throw an error for prepare events
		throw duckdb::NotImplementedException("Prepared transactions are not implemented in DuckDB.");

	case XACT_EVENT_COMMIT:
	case XACT_EVENT_PARALLEL_COMMIT:
		// No action needed for commit event, we already did committed the
		// DuckDB transaction in the PRE_COMMIT event. We don't commit the
		// DuckDB transaction here, because any failure to commit would
		// then turn into a Postgres PANIC (i.e. a crash). To quote the relevant postgres
		// comment:
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
DuckdbXactCallback(XactEvent event, void *arg) {
	InvokeCPPFunc(DuckdbXactCallback_Cpp, event, arg);
}

void
RegisterDuckdbXactCallback() {
	PostgresFunctionGuard(RegisterXactCallback, DuckdbXactCallback, nullptr);
}

} // namespace pgduckdb
