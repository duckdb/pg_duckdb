#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_namespace.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapshot.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "access/genam.h"
#include "access/xact.h"
}

namespace pgduckdb {

PostgresCatalog::PostgresCatalog(duckdb::AttachedDatabase &db, const duckdb::string &connection_string, duckdb::AccessMode access_mode)
	: duckdb::Catalog(db), path(connection_string), access_mode(access_mode) {
}

duckdb::unique_ptr<duckdb::Catalog> PostgresCatalog::Attach(duckdb::StorageExtensionInfo *storage_info, duckdb::ClientContext &context, duckdb::AttachedDatabase &db, const duckdb::string &name, duckdb::AttachInfo &info, duckdb::AccessMode access_mode) {
	duckdb::string connection_string = info.path;

	if (!storage_info) {
		throw duckdb::InternalException("PostgresCatalog should always have access to the PostgresStorageExtensionInfo");
	}
	return duckdb::make_uniq<PostgresCatalog>(db, connection_string, access_mode);
}

// ------------------ Catalog API ---------------------

void PostgresCatalog::Initialize(bool load_builtin) {
	return;
}

string PostgresCatalog::GetCatalogType() {
	return "pgduckdb";
}

optional_ptr<CatalogEntry> PostgresCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw duckdb::NotImplementedException("CreateSchema not supported yet");
}

static Oid LookupSchema(const string &schema_name, Snapshot snapshot) {
	auto rel = table_open(NamespaceRelationId, AccessShareLock);

	ScanKeyData key;
    ScanKeyInit(&key,
                Anum_pg_namespace_nspname,
                BTEqualStrategyNumber,
                NAMEOID,
                CStringGetDatum(schema_name.c_str()));

	auto scan = systable_beginscan(rel, NamespaceNameIndexId, true, snapshot, 1, &key);

	auto tuple = systable_getnext(scan);
	Oid nspoid = InvalidOid;
	if (HeapTupleIsValid(tuple)) {
		nspoid = ((Form_pg_namespace) GETSTRUCT(tuple))->oid;
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return nspoid;
}

optional_ptr<SchemaCatalogEntry> PostgresCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name, OnEntryNotFound if_not_found, QueryErrorContext error_context) {
	if (schema_name == DEFAULT_SCHEMA) {
		return GetSchema(transaction, "public", if_not_found, error_context);
	}

	auto it = schemas.find(schema_name);
	if (it != schemas.end()) {
		return it->second.get();
	}

	auto oid = LookupSchema(schema_name, snapshot);
	if (!OidIsValid(oid)) {
		// Schema could not be found
		return nullptr;
	}

	CreateSchemaInfo create_schema;
	create_schema.schema = schema_name;
	schemas[schema_name] = duckdb::make_uniq<PostgresSchema>(*this, create_schema, snapshot);
	return schemas[schema_name].get();
}

void PostgresCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	throw duckdb::NotImplementedException("ScanSchemas not supported yet");
}

unique_ptr<PhysicalOperator> PostgresCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op, unique_ptr<PhysicalOperator> plan) {
	throw duckdb::NotImplementedException("PlanCreateTableAs not supported yet");
}

unique_ptr<PhysicalOperator> PostgresCatalog::PlanInsert(ClientContext &context, LogicalInsert &op, unique_ptr<PhysicalOperator> plan) {
	throw duckdb::NotImplementedException("PlanInsert not supported yet");
}

unique_ptr<PhysicalOperator> PostgresCatalog::PlanDelete(ClientContext &context, LogicalDelete &op, unique_ptr<PhysicalOperator> plan) {
	throw duckdb::NotImplementedException("PlanDelete not supported yet");
}

unique_ptr<PhysicalOperator> PostgresCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op, unique_ptr<PhysicalOperator> plan) {
	throw duckdb::NotImplementedException("PlanUpdate not supported yet");
}

unique_ptr<LogicalOperator> PostgresCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw duckdb::NotImplementedException("BindCreateIndex not supported yet");
}

DatabaseSize PostgresCatalog::GetDatabaseSize(ClientContext &context) {
	throw duckdb::NotImplementedException("GetDatabaseSize not supported yet");
}

bool PostgresCatalog::InMemory() {
	return false;
}

string PostgresCatalog::GetDBPath() {
	return path;
}

void PostgresCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw duckdb::NotImplementedException("DropSchema not supported yet");
}

} // namespace pgduckdb
