#pragma once

#include "pgduckdb/pg/declarations.hpp"

extern "C" {
namespace pgduckdb {
Oid FindMotherDuckForeignServerOid();

// Return the `default_database` setting defined in the `motherduck` SERVER
// if it exists, returns nullptr otherwise.
const char *FindMotherDuckDefaultDatabase();

// * if `pgduckdb::is_background_worker` then:
//   > returns `sync_token` setting defined in the `motherduck` SERVER if it exists
//   > returns `token` setting defined in the USER MAPPING of the owner of the SERVER if it exists
//   > returns nullptr otherwise
//
// * if not `pgduckdb::is_background_worker` then:
//   > returns `token` setting defined in the USER MAPPING of the current user if it exists
//   > returns nullptr otherwise
const char *FindMotherDuckToken();

const char *FindMotherDuckBackgroundCatalogRefreshInactivityTimeout();

} // namespace pgduckdb
}
