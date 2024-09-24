#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_type.hpp"

extern "C" {
#include "postgres.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
#include "parser/parsetree.h"
}

namespace duckdb {

PostgresType::PostgresType(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info)
    : TypeCatalogEntry(catalog, schema, info) {
}

} // namespace duckdb
