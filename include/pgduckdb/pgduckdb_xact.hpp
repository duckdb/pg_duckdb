namespace pgduckdb {

namespace pg {
bool IsInTransactionBlock();
void PreventInTransactionBlock(const char *statement_type);
} // namespace pg

void ClaimCurrentCommandId();
void RegisterDuckdbXactCallback();
void AutocommitSingleStatementQueries();
void MarkStatementNotTopLevel();
} // namespace pgduckdb
