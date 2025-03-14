extern "C" {
#include "postgres.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"
#include "optimizer/pathnode.h"
#include "utils/builtins.h"
#include "utils/inval.h"
}

#include "pgduckdb/pgduckdb_fdw.hpp"
#include "pgduckdb/pgduckdb_userdata_cache.hpp"

extern "C" {

typedef enum { ANY, MD, S3, GCS, SENTINEL } FdwType;

struct PGDuckDBFdwOption {
	const char *optname;
	FdwType type; /* Type of FDW in which this option is relevant */
	Oid context;  /* Oid of catalog in which option may appear */
	bool required = false;
};

/*
 * Valid options for a pgduckdb_fdw that has a `motherduck` type.
 */
static const struct PGDuckDBFdwOption valid_options[] = {
    /* --- ANY Server --- */
    {"type", ANY, ForeignServerRelationId, true},

    /* --- ANY Mapping --- */
    {"servername", ANY, UserMappingRelationId},

    /* --- MD Server --- */
    // the role bgw assigns to MD tables (by default server creator/owner)
    {"tables_owner_role", MD, ForeignServerRelationId},

    // a specific token to synchronize MD tables, by default the token of the user mapping which belongs creator/owner
    {"sync_token", MD, ForeignServerRelationId},

    {"host", MD, ForeignServerRelationId},             // motherduck_host
    {"port", MD, ForeignServerRelationId},             // motherduck_port
    {"logging", MD, ForeignServerRelationId},          // motherduck_logging
    {"default_database", MD, ForeignServerRelationId}, // Name of the MotherDuck database to be synced (default "my_db")

    /* --- S3 Server --- */
    // TODO
    /* --- S3 Mapping --- */
    // TODO

    /* Sentinel */
    {NULL, SENTINEL, InvalidOid}};

PG_FUNCTION_INFO_V1(pgduckdb_fdw_handler);
Datum
pgduckdb_fdw_handler(PG_FUNCTION_ARGS __attribute__((unused))) {
	PG_RETURN_POINTER(nullptr);
}

static const char *
find_option(List *options_list, const char *name) {
	foreach_node(DefElem, def, options_list) {
		if (strcmp(def->defname, name) == 0) {
			return defGetString(def);
		}
	}
	return NULL;
}

static const char *
get_option(List *options_list, const char *name) {
	auto val = find_option(options_list, name);
	if (val == NULL) {
		elog(ERROR, "Missing required option: '%s'", name);
	}
	return val;
}

FdwType
get_fdw_type(const char *type_str) {
	if (strcmp(type_str, "motherduck") == 0) {
		return MD;
	} else if (strcmp(type_str, "s3") == 0) {
		return S3;
	} else if (strcmp(type_str, "gcs") == 0) {
		return GCS;
	} else {
		elog(ERROR, "Unknown SERVER type: '%s'", type_str);
		return SENTINEL; // unreachable
	}
}

bool
option_match_type(const struct PGDuckDBFdwOption *opt, FdwType type) {
	return opt->type == ANY || opt->type == type;
}

bool
is_valid_option(const char *optname, FdwType type, Oid context) {
	for (const struct PGDuckDBFdwOption *opt = valid_options; opt->optname != NULL; ++opt) {
		if (option_match_type(opt, type) && opt->context == context && strcmp(optname, opt->optname) == 0) {
			return true;
		}
	}
	return false;
}

void
validate_has_required_options(List *options_list, FdwType type, Oid context) {
	for (const struct PGDuckDBFdwOption *opt = valid_options; opt->optname != NULL; ++opt) {
		if (!opt->required || opt->context != context || option_match_type(opt, type)) {
			continue;
		}

		bool found = false;
		foreach_node(DefElem, def, options_list) {
			if (strcmp(def->defname, opt->optname) == 0) {
				found = true;
				break;
			}
		}
		if (!found) {
			elog(ERROR, "Missing required option: '%s'", opt->optname);
		}
	}
}

namespace pgduckdb {

namespace {
Oid
FindOid(const char *query) {
	SPI_connect();
	int ret = SPI_exec(query, 0);

	if (ret != SPI_OK_SELECT) {
		elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
	}

	Oid oid = InvalidOid;
	if (SPI_processed > 0) {
		HeapTuple tuple = SPI_tuptable->vals[0];
		bool isnull = false;
		Datum oid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);

		if (isnull) {
			elog(ERROR, "FATAL: Expected oid to be returned, but found NULL");
		}

		oid = DatumGetObjectId(oid_datum);
	}

	SPI_finish();
	return oid;
}
} // namespace

Oid
FindMotherDuckForeignServerOid() {
	return FindOid(R"(
		SELECT fs.oid
		FROM pg_foreign_server fs
		INNER JOIN pg_foreign_data_wrapper fdw ON fdw.oid = fs.srvfdw
		WHERE fdw.fdwname = 'pg_duckdb' AND fs.srvtype = 'motherduck';
	)");
}

Oid
FindMotherDuckUserMappingOid() {
	return FindOid(R"(
		SELECT um.oid
		FROM pg_foreign_server fs
		INNER JOIN pg_foreign_data_wrapper fdw ON fdw.oid = fs.srvfdw
		INNER JOIN pg_user_mapping um ON um.umserver = fs.oid
		WHERE fdw.fdwname = 'pg_duckdb' AND fs.srvtype = 'motherduck';
	)");
}

} // namespace pgduckdb

void
validate_has_no_motherduck_foreign_server() {
	auto oid = pgduckdb::FindMotherDuckForeignServerOid();
	if (oid != InvalidOid) {
		auto name = GetForeignServer(oid)->servername;
		elog(ERROR, "MotherDuck FDW already exists ('%s')", name);
	}
}

void
assert_has_no_motherduck_user_mapping() {
	auto oid = pgduckdb::FindMotherDuckUserMappingOid();
	if (oid != InvalidOid) {
		elog(ERROR, "MotherDuck USER MAPPING already exists");
	}
}

void
validate_options(List *options_list, FdwType type, Oid context) {
	// Make sure all options are valid
	foreach_node(DefElem, def, options_list) {
		if (!is_valid_option(def->defname, type, context)) {
			elog(ERROR, "Unknown option: '%s'", def->defname);
		}
	}

	// Make sure we have all required options
	validate_has_required_options(options_list, type, context);
}

Datum
validate_motherduck_server_fdw(List *options_list, Oid context) {
	// For now only accept one MotherDuck FDW globally
	// can be relaxed eventually with https://github.com/duckdb/pg_duckdb/pull/545

	// TODO: take a global lock to make this check
	validate_has_no_motherduck_foreign_server();

	validate_options(options_list, MD, context);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pgduckdb_fdw_validator);
Datum
pgduckdb_fdw_validator(PG_FUNCTION_ARGS) {
	Oid catalog = PG_GETARG_OID(1);

	if (catalog == ForeignTableRelationId || catalog == ForeignTableRelidIndexId) {
		elog(ERROR, "pgduckdb FDW only support 'SERVER' and 'USER MAPPING' objects.");
	}

	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	if (catalog == ForeignDataWrapperRelationId) {
		// `CREATE FOREIGN DATA WRAPPER` statement
		foreach_node(DefElem, def, options_list) {
			elog(ERROR, "pg_duckdb FDW does not take any option, found '%s'", def->defname);
		}

		PG_RETURN_VOID(); // no additional validation needed
	} else if (catalog == ForeignServerRelationId) {
		auto type_str = get_option(options_list, "type");
		auto type = get_fdw_type(type_str);
		// TODO? remove the "type" from the server "options" somehow?

		switch (type) {
		case MD:
			return validate_motherduck_server_fdw(options_list, catalog);
		case S3:
		case GCS:
			elog(ERROR, "Not implemented.");
		default: // unreachable
			elog(ERROR, "Unknown type: %d", type);
		}
		PG_RETURN_VOID();
	} else if (catalog == UserMappingRelationId) {
		auto servername = get_option(options_list, "servername");
		auto server = GetForeignServerByName(servername, false);
		auto server_type = get_fdw_type(server->servertype);
		validate_options(options_list, server_type, catalog);

		if (server_type == MD) {
			// For now only accept one USER MAPPING for MD SERVERs
			assert_has_no_motherduck_user_mapping();
		}

		// For now we chose to not register a callback for USER MAPPING
		// because PG has a hard limit of 64. So we're explicitely invalidating it here.
		pgduckdb::InvalidateUserDataCache();

		PG_RETURN_VOID();
	}

	elog(ERROR, "Unknown catalog: %d", catalog);

	PG_RETURN_VOID();
}

} // extern "C"
