#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"

extern "C" {
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
#include "executor/nodeIndexscan.h"
#include "nodes/pathnodes.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/rel.h"
}

namespace duckdb {

PostgresTransaction::PostgresTransaction(TransactionManager &manager, ClientContext &context, PostgresCatalog &catalog,
                                         Snapshot snapshot)
    : Transaction(manager, context), catalog(catalog), snapshot(snapshot) {
}

PostgresTransaction::~PostgresTransaction() {
}

optional_ptr<CatalogEntry>
SchemaItems::GetTable(const string &entry_name) {
	auto it = tables.find(entry_name);
	if (it != tables.end()) {
		return it->second.get();
	}

	auto snapshot = schema->snapshot;
	auto &catalog = schema->catalog;

	List *name_list = NIL;
	name_list = lappend(name_list, makeString(pstrdup(name.c_str())));
	name_list = lappend(name_list, makeString(pstrdup(entry_name.c_str())));

	RangeVar *table_range_var = makeRangeVarFromNameList(name_list);
	Oid rel_oid = RangeVarGetRelid(table_range_var, AccessShareLock, true);
	if (rel_oid == InvalidOid) {
		// Table could not be found
		return nullptr;
	}

	// Check if the Relation is a VIEW
	auto tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_oid));
	if (!HeapTupleIsValid(tuple)) {
		throw std::runtime_error("Cache lookup failed for relation " + std::to_string(rel_oid));
	}

	auto relForm = (Form_pg_class)GETSTRUCT(tuple);

	// Check if the relation is a view
	if (relForm->relkind == RELKIND_VIEW) {
		ReleaseSysCache(tuple);
		// Let the replacement scan handle this, the ReplacementScan replaces the view with its view_definition, which
		// will get bound again and hit a PostgresIndexTable / PostgresHeapTable.
		return nullptr;
	}

	ReleaseSysCache(tuple);

	::Relation rel = PostgresTable::OpenRelation(rel_oid);

	CreateTableInfo info;
	info.table = entry_name;
	PostgresTable::SetTableInfo(info, rel);

	auto cardinality = PostgresTable::GetTableCardinality(rel);
	auto table = make_uniq<PostgresHeapTable>(catalog, *schema, info, rel, cardinality, snapshot);
	tables[entry_name] = std::move(table);
	return tables[entry_name].get();
}

optional_ptr<CatalogEntry> SchemaItems::GetSchema() const {
	return schema.get();
}

optional_ptr<CatalogEntry>
PostgresTransaction::GetSchema(const string &name) {
	auto it = schemas.find(name);
	if (it != schemas.end()) {
		return it->second.GetSchema();
	}

	CreateSchemaInfo create_schema;
	create_schema.schema = name;
	auto pg_schema = make_uniq<PostgresSchema>(catalog, create_schema, snapshot);
	schemas.emplace(std::make_pair(name, SchemaItems(std::move(pg_schema), name)));
	return schemas.at(name).GetSchema();
}

optional_ptr<CatalogEntry>
PostgresTransaction::GetCatalogEntry(CatalogType type, const string &schema, const string &name) {
	switch (type) {
	case CatalogType::TABLE_ENTRY: {
		auto it = schemas.find(schema);
		if (it == schemas.end()) {
			return nullptr;
		}

		auto &schema_entry = it->second;
		return schema_entry.GetTable(name);
	}
	case CatalogType::SCHEMA_ENTRY: {
		return GetSchema(schema);
	}
	default:
		return nullptr;
	}
}

} // namespace duckdb

// namespace duckdb
