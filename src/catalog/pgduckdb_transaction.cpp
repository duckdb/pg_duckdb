#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"

extern "C" {
#include "catalog/namespace.h" // makeRangeVarFromNameList
#include "utils/syscache.h"    // RELOID
#include "utils/rel.h"         // Form_pg_class, RELKIND_VIEW
}

namespace pgduckdb {

PostgresTransaction::PostgresTransaction(duckdb::TransactionManager &manager, duckdb::ClientContext &context,
                                         PostgresCatalog &catalog, Snapshot snapshot)
    : duckdb::Transaction(manager, context), catalog(catalog), snapshot(snapshot) {
}

PostgresTransaction::~PostgresTransaction() {
}

SchemaItems::SchemaItems(duckdb::unique_ptr<PostgresSchema> &&schema, const duckdb::string &name)
	: name(name), schema(std::move(schema)) {
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SchemaItems::GetTable(const duckdb::string &entry_name) {
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
		throw duckdb::CatalogException("Cache lookup failed for relation %u", rel_oid);
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

	Relation rel = PostgresTable::OpenRelation(rel_oid);

	duckdb::CreateTableInfo info;
	info.table = entry_name;
	PostgresTable::SetTableInfo(info, rel);

	auto cardinality = PostgresTable::GetTableCardinality(rel);
	auto table = duckdb::make_uniq<PostgresHeapTable>(catalog, *schema, info, rel, cardinality, snapshot);
	tables[entry_name] = std::move(table);
	return tables[entry_name].get();
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SchemaItems::GetSchema() const {
	return schema.get();
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresTransaction::GetSchema(const duckdb::string &name) {
	auto it = schemas.find(name);
	if (it != schemas.end()) {
		return it->second.GetSchema();
	}

	duckdb::CreateSchemaInfo create_schema;
	create_schema.schema = name;
	auto pg_schema = duckdb::make_uniq<PostgresSchema>(catalog, create_schema, snapshot);
	schemas.emplace(std::make_pair(name, SchemaItems(std::move(pg_schema), name)));
	return schemas.at(name).GetSchema();
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresTransaction::GetCatalogEntry(duckdb::CatalogType type, const duckdb::string &schema,
                                     const duckdb::string &name) {
	switch (type) {
	case duckdb::CatalogType::TABLE_ENTRY: {
		auto it = schemas.find(schema);
		if (it == schemas.end()) {
			return nullptr;
		}

		auto &schema_entry = it->second;
		return schema_entry.GetTable(name);
	}
	case duckdb::CatalogType::SCHEMA_ENTRY: {
		return GetSchema(schema);
	}
	default:
		return nullptr;
	}
}

} // namespace pgduckdb
