#pragma once

#include "pgduckdb/pg/declarations.hpp"

extern "C" {
namespace pgduckdb {

Oid FindMotherDuckForeignServerOid();

/*
    Returns the Oid of the USER MAPPING for the given user and server.
    Returns InvalidOid if no such mapping exists.

    Note: we cannot use PG's `GetUserMapping` because:
    - it checks the global scope if not found
    - it throws an error if no mapping is not found
*/
Oid FindUserMappingForUser(Oid user_oid, Oid server_oid);

// Return the `default_database` setting defined in the `motherduck` SERVER
// if it exists, returns nullptr otherwise.
const char *FindMotherDuckDefaultDatabase();

// * if `pgduckdb::is_background_worker` then:
//   > returns `token` option defined in the USER MAPPING of the owner of the SERVER if it exists
//   > returns nullptr otherwise
//
// * if not `pgduckdb::is_background_worker` then:
//   > returns `token` option defined in the USER MAPPING of the current user if it exists
//   > returns nullptr otherwise
const char *FindMotherDuckToken();

const char *FindMotherDuckBackgroundCatalogRefreshInactivityTimeout();

} // namespace pgduckdb
}
