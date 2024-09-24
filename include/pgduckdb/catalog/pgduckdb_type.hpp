#pragma once

#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"

extern "C" {
#include "postgres.h"
#include "utils/snapshot.h"
#include "postgres.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
}

namespace duckdb {

class PostgresType : public TypeCatalogEntry {
public:
	~PostgresType() {
	}
	PostgresType(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info);
};

} // namespace duckdb
