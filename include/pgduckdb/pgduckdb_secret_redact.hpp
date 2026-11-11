#pragma once

extern "C" {
struct ErrorData;
struct Query;
}

namespace pgduckdb {

/*
 * Called from the emit_log_hook: cuts the statement and the other texts of a
 * log entry after the first word that hints at a secret (outside of string
 * literals), unless duckdb.allow_unredacted_secrets is enabled.
 */
void RedactSecretsInLogMessage(ErrorData *edata);

/*
 * Returns true for utility statements whose text contains secrets that are
 * stored verbatim by query stats extensions like pg_stat_statements.
 */
bool IsSecretUtilityStatement(Query *query);

} // namespace pgduckdb
