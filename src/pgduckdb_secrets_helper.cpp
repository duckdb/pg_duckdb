#include "pgduckdb/pgduckdb_secrets_helper.hpp"

#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "pgduckdb/pg/string_utils.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_authid_d.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/syscache.h"

#include "pgduckdb/vendor/pg_list.hpp"
}

namespace pgduckdb {
namespace pg {

const char *
FindOption(List *options_list, const char *name) {
	foreach_node(DefElem, def, options_list) {
		if (strcmp(def->defname, name) == 0) {
			return defGetString(def);
		}
	}
	return NULL;
}

const char *
GetOption(List *options_list, const char *name) {
	auto val = FindOption(options_list, name);
	if (val == NULL) {
		elog(ERROR, "Missing required option: '%s'", name);
	}
	return val;
}

namespace {

bool
appendOptions(StringInfoData &buf, List *options) {
	bool first = true;
	foreach_node(DefElem, def, options) {
		if (first) {
			first = false;
		} else {
			appendStringInfoString(&buf, ", ");
		}

		// No need to sanitize option's name since it went through PG's validation
		appendStringInfo(&buf, "%s %s", def->defname, quote_literal_cstr(defGetString(def)));
	}

	return !first;
}

const char *
MakeSecretName(const char *server_name, Oid um_user_oid) {
	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "pgduckdb_secret_%s", server_name);
	if (um_user_oid != InvalidOid) {
		appendStringInfo(&buf, "_%u", um_user_oid);
	}
	return buf.data;
}

char *
MakeDuckDBCreateSecretQuery(const char *secret_name, const char *type, List *server_options,
                            List *mapping_options = nullptr) {
	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "CREATE SECRET %s (", secret_name);

	bool appended_options = appendOptions(buf, server_options);
	if (list_length(mapping_options) > 0) {
		if (appended_options) {
			appendStringInfoString(&buf, ", ");
		}
		appended_options = appendOptions(buf, mapping_options);
	}

	if (appended_options) {
		appendStringInfoString(&buf, ", ");
	}

	appendStringInfo(&buf, "TYPE %s", type);

	appendStringInfo(&buf, ")");
	return buf.data;
}

} // namespace

List *
ListDuckDBCreateSecretQueries() {
	MemoryContext entry_ctx = CurrentMemoryContext;
	SPI_connect();
	auto query = R"(
		SELECT
			fs.oid as server_oid,
			fs.srvtype as server_type,
			um.umuser as um_user_oid
		FROM pg_foreign_server fs
		INNER JOIN pg_foreign_data_wrapper fdw ON fdw.oid = fs.srvfdw
		LEFT JOIN pg_user_mapping um ON um.umserver = fs.oid
		WHERE fdw.fdwname = 'duckdb' AND fs.srvtype != 'motherduck';
	)";

	// Escalate privileges to superuser to read the pg_user_mapping table
	// XXX: Let's rediscuss this point to make sure we want to share secrets like this across users.
	Oid saved_userid;
	int sec_context;
	GetUserIdAndSecContext(&saved_userid, &sec_context);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, sec_context | SECURITY_LOCAL_USERID_CHANGE);

	auto ret = SPI_exec(query, 0);
	if (ret != SPI_OK_SELECT) {
		elog(ERROR, "Can't list DuckDB secrets: %s", SPI_result_code_string(ret));
	}

	SetUserIdAndSecContext(saved_userid, sec_context);

	List *results = NIL;

	for (uint64_t i = 0; i < SPI_processed; ++i) {
		HeapTuple tuple = SPI_tuptable->vals[i];
		bool is_serveroid_null = false;
		bool is_umoid_null = false;
		bool is_servertype_null = false;
		Datum server_oid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &is_serveroid_null);
		Datum server_type_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &is_servertype_null);
		Datum um_user_oid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 3, &is_umoid_null);

		if (is_serveroid_null) {
			elog(ERROR, "FATAL: Expected server oid to be returned, but found NULL");
		} else if (is_servertype_null) {
			elog(ERROR, "FATAL: Expected server type to be returned, but found NULL");
		}

		// Get USER MAPPING if it exists
		const Oid server_oid = DatumGetObjectId(server_oid_datum);
		const char *type = TextDatumGetCString(server_type_datum);
		Oid um_user_oid = InvalidOid;
		List *user_mapping_options = nullptr;
		if (!is_umoid_null) {
			um_user_oid = DatumGetObjectId(um_user_oid_datum);
			auto user_mapping = FindUserMapping(um_user_oid, server_oid, true);
			if (user_mapping == nullptr) {
				// Concurrently deleted? Should we run this under locks?
				elog(ERROR, "FATAL: could not find USER MAPPING for user %u and server %u", um_user_oid, server_oid);
			}

			user_mapping_options = user_mapping->options;
		}

		// Get SERVER
		auto server = GetForeignServer(server_oid);
		if (server == nullptr) {
			elog(ERROR, "FATAL: could not find FOREIGN SERVER with oid %u", server_oid);
		}

		auto secret_name = MakeSecretName(server->servername, um_user_oid);
		{
			// Enter memory context preceding the SPI call so that it survives `SPI_finish`
			MemoryContext spi_mem_ctx = MemoryContextSwitchTo(entry_ctx);
			auto secret_query = MakeDuckDBCreateSecretQuery(secret_name, type, server->options, user_mapping_options);
			results = lappend(results, makeString(secret_query));
			MemoryContextSwitchTo(spi_mem_ctx);
		}
	}

	SPI_finish();
	return results;
}

/*
Modified version of Postgres' GetUserMapping:
- doesn't check in the PUBLIC namespace
- returns nullptr if it doesn't exist (doesn't throw an error)
- optionally fetches the options
*/
UserMapping *
FindUserMapping(Oid userid, Oid serverid, bool with_options) {
	HeapTuple tp = SearchSysCache2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(userid), ObjectIdGetDatum(serverid));
	if (!HeapTupleIsValid(tp)) {
		return nullptr;
	}

	UserMapping *um = (UserMapping *)palloc(sizeof(UserMapping));
	um->umid = ((Form_pg_user_mapping)GETSTRUCT(tp))->oid;
	um->userid = userid;
	um->serverid = serverid;
	um->options = NIL;

	if (!with_options) {
		ReleaseSysCache(tp);
		return um;
	}

	/* Extract the umoptions */
	bool isnull;
	Datum datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp, Anum_pg_user_mapping_umoptions, &isnull);
	if (!isnull) {
		um->options = untransformRelOptions(datum);
	}

	ReleaseSysCache(tp);
	return um;
}

const char *
GetQueryError(const char *query, List* server_options) {
	// Create a new connection on the DB so we don't refresh secrets or anything
	auto &db = pgduckdb::DuckDBManager::Get().GetDatabase();
	duckdb::Connection con(db);

	auto tx_query = duckdb::StringUtil::Format("BEGIN; %s", query);
	auto res = con.Query(tx_query);
	if (res->HasError()) {
		con.Query("ROLLBACK;");
		return pstrdup(res->GetErrorObject().RawMessage().c_str());
	}

	auto& secret_manager = duckdb::SecretManager::Get(*con.context);
	auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(*con.context);
	auto secret_entry = secret_manager.GetSecretByName(transaction, "new_pgduckdb_secret");
	if (!secret_entry) {
		return "FATAL: Failed to get secret";
	} else if (!secret_entry->secret) {
		return "FATAL: No secret attached to the entry";
	}

	auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret*>(secret_entry->secret.get());
	if (!kv_secret) {
		return "FATAL: Secret is not a duckdb::KeyValueSecret";
	}

	// Make sure we're not using restricted options in the server
	std::vector<std::string> restricted;
	for (const auto& k : kv_secret->redact_keys) {
		if (FindOption(server_options, k.c_str()) != NULL) {
			restricted.push_back(k);
		}
	}

	if (restricted.size() == 0) {
		return nullptr;
	}

	std::ostringstream oss;
	oss << (restricted.size() == 1 ? "Option " : "Options ");
	for (size_t i = 0; i < restricted.size(); ++i) {
		oss << "'" << restricted[i] << "'";
		if (i != restricted.size() - 1) {
			oss << ", ";
		}
	}
	oss << " cannot be used in the SERVER's OPTIONS, please move it to the USER MAPPING";

	con.Query("ROLLBACK;");
	return pstrdup(oss.str().c_str());
}

void
ValidateDuckDBSecret(const char *type, List *server_options, List *mapping_options) {
	if (type == nullptr) {
		elog(ERROR, "Missing required option: 'type'");
	}

	auto query = MakeDuckDBCreateSecretQuery("new_pgduckdb_secret", type, server_options, mapping_options);
	auto err = InvokeCPPFunc(GetQueryError, query, server_options);
	if (err != nullptr) {
		elog(ERROR, "%s", err);
	}
}
} // namespace pg
} // namespace pgduckdb
