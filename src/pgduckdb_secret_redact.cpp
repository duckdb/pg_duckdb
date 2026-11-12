#include "pgduckdb/pgduckdb_secret_redact.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"

extern "C" {
#include "postgres.h"

#include "foreign/foreign.h"
#include "nodes/parsenodes.h"
#include "tcop/tcopprot.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
}

#include "pgduckdb/pgduckdb_guc.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"

namespace pgduckdb {

namespace {

/* Secrets are passed after one of these (lowercase) words, e.g. in USER MAPPING OPTIONS or as function arguments */
const char *const SECRET_HINTS[] = {"connection_string", "key_id", "mapping", "motherduck",
                                    "password",          "secret", "token"};

const char *const REDACTED =
    " <redacted by pg_duckdb because it may contain secrets, see duckdb.allow_unredacted_secrets option>";

/* A copy of the redacted debug_query_string, in TopMemoryContext */
char *redacted_query_string = nullptr;

bool
StartsWithIgnoringCase(const char *str, const char *lowercase_prefix) {
	for (; *lowercase_prefix != '\0'; str++, lowercase_prefix++) {
		if (duckdb::StringUtil::CharacterToLower(*str) != *lowercase_prefix) {
			return false;
		}
	}
	return true;
}

/* Returns the end of the first secret hint that starts in [begin, end), or NULL */
const char *
FindHint(const char *begin, const char *end) {
	for (const char *p = begin; p < end; p++) {
		for (const char *hint : SECRET_HINTS) {
			if (StartsWithIgnoringCase(p, hint)) {
				return p + strlen(hint);
			}
		}
	}
	return nullptr;
}

/* Whether the token starts with the word, ignoring case */
bool
IsWord(const char *token, const char *lowercase_word) {
	char next = token[strlen(lowercase_word)];
	return StartsWithIgnoringCase(token, lowercase_word) && !duckdb::StringUtil::CharacterIsAlphaNumeric(next) &&
	       next != '_';
}

using TokenType = duckdb::SimplifiedTokenType;

/* What we know about the statement at the current token, to tell if a string contains code */
struct CodeContext {
	bool statement_start = true;
	/* The only string of a DO statement is its code */
	bool do_statement = false;
	/* The previous token, except for comments */
	const char *prev = "";
	int paren_depth = 0;
	/* paren_depth of the arguments of duckdb.raw_query() or duckdb.query(), -1 outside of them */
	int code_call_depth = -1;
};

bool
IsDoStatement(const char *first_token) {
	return IsWord(first_token, "do");
}

/* Functions whose arguments are code, the schema is not checked as it can be in the search_path */
bool
IsCodeFunction(const char *name) {
	return IsWord(name, "raw_query") || IsWord(name, "query");
}

/* Strings are data, except for the code of a DO statement, a function body after AS and DuckDB queries */
bool
IsCodeString(const CodeContext &context) {
	return context.do_statement || IsWord(context.prev, "as") || context.code_call_depth >= 0;
}

void
TrackParens(CodeContext &context, char op) {
	if (op == '(') {
		context.paren_depth++;
		if (context.code_call_depth < 0 && IsCodeFunction(context.prev)) {
			context.code_call_depth = context.paren_depth;
		}
	} else if (op == ')') {
		if (context.paren_depth == context.code_call_depth) {
			context.code_call_depth = -1;
		}
		context.paren_depth--;
	} else if (op == ';') {
		context = CodeContext();
	}
}

/* Updates the context after a token, except for comments */
void
TrackToken(CodeContext &context, TokenType type, const char *token) {
	if (context.statement_start) {
		context.do_statement = IsDoStatement(token);
		context.statement_start = false;
	}
	if (type == TokenType::SIMPLIFIED_TOKEN_OPERATOR) {
		TrackParens(context, *token);
	}
	if (!context.statement_start) {
		context.prev = token;
	}
}

/*
 * Returns the end of the first secret hint outside of string literals, or in
 * strings that contain code, or NULL if there is none. Throws if DuckDB's
 * tokenizer fails, which it never does on invalid input.
 */
const char *
FindHintInCode(const char *text) {
	auto tokens = duckdb::Parser::Tokenize(text);
	CodeContext context;
	for (size_t i = 0; i < tokens.size(); i++) {
		auto type = tokens[i].type;
		const char *token = text + tokens[i].start;
		if (type != TokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT || IsCodeString(context)) {
			size_t end = i + 1 < tokens.size() ? tokens[i + 1].start : strlen(text);
			const char *hint_end = FindHint(token, text + end);
			if (hint_end) {
				return hint_end;
			}
		}
		if (type != TokenType::SIMPLIFIED_TOKEN_COMMENT) {
			TrackToken(context, type, token);
		}
	}
	return nullptr;
}

/*
 * Returns a palloc'd copy of text that is cut after the first secret hint
 * found by FindHintInCode, or NULL if there is none. This also works for
 * queries that fail to parse.
 */
char *
Redact(const char *text) {
	if (text == nullptr || FindHint(text, text + strlen(text)) == nullptr) {
		return nullptr;
	}

	const char *hint_end;
	try {
		hint_end = FindHintInCode(text);
	} catch (...) {
		hint_end = text;
	}

	if (hint_end == nullptr) {
		return nullptr;
	}
	return psprintf("%.*s%s", static_cast<int>(hint_end - text), text, REDACTED);
}

void
RedactField(char **field) {
	char *redacted = Redact(*field);
	if (redacted) {
		*field = redacted;
	}
}

} // namespace

void
RedactSecretsInLogMessage(ErrorData *edata) {
	if (duckdb_allow_unredacted_secrets) {
		return;
	}

	/*
	 * Postgres adds debug_query_string to the server log as the STATEMENT of
	 * failed queries, so make it point to a redacted copy. Postgres resets it
	 * when the next query starts.
	 */
	if (debug_query_string != nullptr && debug_query_string != redacted_query_string) {
		char *redacted = Redact(debug_query_string);
		if (redacted) {
			if (redacted_query_string) {
				pfree(redacted_query_string);
			}
			redacted_query_string = MemoryContextStrdup(TopMemoryContext, redacted);
			debug_query_string = redacted_query_string;
		}
	}

	/* Queries run with SPI, like the ones of our secret helpers, are in the context or the internal query */
	RedactField(&edata->context);
	RedactField(&edata->internalquery);
	/* Only goes to the server log, e.g. a deadlock report lists the statements of the other sessions in it */
	RedactField(&edata->detail_log);

	/*
	 * After the swap above, debug_query_string points to redacted_query_string
	 * exactly when the current statement was redacted, either for this log
	 * entry or for an earlier one of the same statement (Postgres only resets
	 * debug_query_string when the next statement starts). Only then can the
	 * message and the detail contain secrets from the statement. The NULL check
	 * is needed because both are NULL before any statement was redacted.
	 */
	if (debug_query_string == nullptr || debug_query_string != redacted_query_string) {
		return;
	}

	/*
	 * Statement logging messages (which hide the STATEMENT) contain the
	 * statement, and their detail has the values of the bind parameters.
	 * Syntax errors quote a part of the statement. Other messages are kept, as
	 * they are also sent to the client.
	 */
	if (edata->hide_stmt || edata->sqlerrcode == ERRCODE_SYNTAX_ERROR) {
		RedactField(&edata->message);
	}
	if (edata->hide_stmt && edata->detail) {
		edata->detail = pstrdup(REDACTED + 1);
	}
}

bool
IsSecretUtilityStatement(Query *query) {
	Node *stmt = query->utilityStmt;
	if (stmt == nullptr) {
		return false;
	}

	const char *server_name = nullptr;
	List *options = NIL;
	if (IsA(stmt, CreateUserMappingStmt)) {
		auto create_stmt = castNode(CreateUserMappingStmt, stmt);
		server_name = create_stmt->servername;
		options = create_stmt->options;
	} else if (IsA(stmt, AlterUserMappingStmt)) {
		auto alter_stmt = castNode(AlterUserMappingStmt, stmt);
		server_name = alter_stmt->servername;
		options = alter_stmt->options;
	} else if (IsA(stmt, CallStmt)) {
		auto call_stmt = castNode(CallStmt, stmt);
		if (call_stmt->funcexpr == nullptr || !IsExtensionRegistered()) {
			return false;
		}
		Oid func_oid = call_stmt->funcexpr->funcid;
		const char *func_name = get_func_name(func_oid);
		const char *schema_name = get_namespace_name(get_func_namespace(func_oid));
		return func_name && schema_name && strcmp(schema_name, "duckdb") == 0 &&
		       strcmp(func_name, "enable_motherduck") == 0;
	} else {
		return false;
	}

	if (options == NIL || !IsExtensionRegistered()) {
		return false;
	}

	auto server = GetForeignServerByName(server_name, true);
	return server && server->fdwid == get_foreign_data_wrapper_oid("duckdb", true);
}

} // namespace pgduckdb
