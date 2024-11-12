namespace pgduckdb {
void ClaimCurrentCommandId();
void RegisterDuckdbXactCallback();
void AutocommitSingleStatementQueries();
void MarkStatementNotTopLevel();
bool IsInTransactionBlock();
void PreventInTransactionBlock(const char *statement_type);
} // namespace pgduckdb
