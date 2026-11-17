#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "access/xact.h" // RegisterXactCallback, XactEvent, SubXactEvent, SubTransactionId
#include "access/xlog.h" // XactLastRecEnd
}

#include "pgduckdb/pg/transactions.hpp"

namespace pgduckdb::pg {

CommandId
GetCurrentCommandId(bool used) {
	return PostgresFunctionGuard(::GetCurrentCommandId, used);
}

void
CommandCounterIncrement() {
	return PostgresFunctionGuard(::CommandCounterIncrement);
}

bool
IsInTransactionBlock(bool is_top_level) {
	return PostgresFunctionGuard(::IsInTransactionBlock, is_top_level);
}

/*
 * Returns true if the current transaction accessed any temporary relations.
 * Postgres uses the same check to decide whether it needs to truncate ON
 * COMMIT DELETE ROWS tables.
 */
bool
AccessedTempNamespace() {
	return (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPNAMESPACE) != 0;
}

void
PreventInTransactionBlock(bool is_top_level, const char *statement_type) {
	return PostgresFunctionGuard(::PreventInTransactionBlock, is_top_level, statement_type);
}

void
RegisterXactCallback(XactCallback callback, void *arg) {
	return PostgresFunctionGuard(::RegisterXactCallback, callback, arg);
}

void
UnregisterXactCallback(XactCallback callback, void *arg) {
	return PostgresFunctionGuard(::UnregisterXactCallback, callback, arg);
}

void
RegisterSubXactCallback(SubXactCallback callback, void *arg) {
	return PostgresFunctionGuard(::RegisterSubXactCallback, callback, arg);
}

void
UnregisterSubXactCallback(SubXactCallback callback, void *arg) {
	return PostgresFunctionGuard(::UnregisterSubXactCallback, callback, arg);
}

} // namespace pgduckdb::pg
