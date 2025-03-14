#pragma once

#include "pgduckdb/pg/declarations.hpp"

extern "C" {
namespace pgduckdb {
Oid FindMotherDuckForeignServerOid();
Oid FindMotherDuckUserMappingOid();
} // namespace pgduckdb
}
