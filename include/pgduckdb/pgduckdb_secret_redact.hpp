#pragma once

extern "C" {
struct ErrorData;
}

namespace pgduckdb {

/*
 * Called from the emit_log_hook: cuts the statement and the other texts of a
 * log entry after the first word that hints at a secret (outside of string
 * literals), unless duckdb.allow_unredacted_secrets is enabled.
 */
void RedactSecretsInLogMessage(ErrorData *edata);

} // namespace pgduckdb
