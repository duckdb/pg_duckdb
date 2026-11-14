#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

namespace pg {
bool IsInTransactionBlock();
void PreventInTransactionBlock(const char *statement_type);
void SetForceAllowWrites(bool force);
bool AllowWrites();
} // namespace pg

bool MixedWritesAllowed();
bool DidDisallowedMixedWrites();
void CheckForDisallowedMixedWrites();
void ClaimCurrentCommandId(bool force = false);
void ClaimWalWrites();
bool DidUnclaimedWalWrites();
void RegisterDuckdbTempTable(Oid relid, bool on_commit_delete_rows);
void UnregisterDuckdbTempTable(Oid relid);
bool IsDuckdbTempTable(Oid relid);
bool OnCommitDeleteRowsDone();
void RegisterDuckdbXactCallback();
void AutocommitSingleStatementQueries();
void MarkStatementNotTopLevel();
void SetStatementTopLevel(bool top_level);
bool IsStatementTopLevel();
} // namespace pgduckdb
