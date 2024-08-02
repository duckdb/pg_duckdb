
#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
}

#include "pgduckdb/pgduckdb_options.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"

namespace pgduckdb {

static Oid
duckdbGetNamespace(void) {
	return get_namespace_oid("duckdb", false);
}

static Oid
duckdbSecretsRelationId(void) {
	return get_relname_relid("secrets", duckdbGetNamespace());
}

static Oid
duckdbExtensionsRelationId(void) {
	return get_relname_relid("extensions", duckdbGetNamespace());
}

static std::string
duckdbDatumToString(Datum datum) {
	std::string columnValue;
	text *cloudType = DatumGetTextPP(datum);
	columnValue = VARDATA_ANY(cloudType);
	columnValue.resize(VARSIZE_ANY_EXHDR(cloudType));
	return columnValue;
}

std::vector<DuckdbSecret>
ReadDuckdbSecrets() {
	HeapTuple tuple = NULL;
	Oid duckdbSecretRelationId = duckdbSecretsRelationId();
	Relation duckdbSecretRelation = table_open(duckdbSecretRelationId, AccessShareLock);
	SysScanDescData *scan = systable_beginscan(duckdbSecretRelation, InvalidOid, false, GetActiveSnapshot(), 0, NULL);
	std::vector<DuckdbSecret> duckdbSecrets;

	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		Datum datumArray[Natts_duckdb_secret];
		bool isNullArray[Natts_duckdb_secret];

		heap_deform_tuple(tuple, RelationGetDescr(duckdbSecretRelation), datumArray, isNullArray);
		DuckdbSecret secret;

		secret.type = duckdbDatumToString(datumArray[Anum_duckdb_secret_type - 1]);
		secret.id = duckdbDatumToString(datumArray[Anum_duckdb_secret_id - 1]);
		secret.secret = duckdbDatumToString(datumArray[Anum_duckdb_secret_secret - 1]);

		if (!isNullArray[Anum_duckdb_secret_region - 1])
			secret.region = duckdbDatumToString(datumArray[Anum_duckdb_secret_region - 1]);

		if (!isNullArray[Anum_duckdb_secret_endpoint - 1])
			secret.endpoint = duckdbDatumToString(datumArray[Anum_duckdb_secret_endpoint - 1]);

		if (!isNullArray[Anum_duckdb_secret_r2_account_id - 1])
			secret.endpoint = duckdbDatumToString(datumArray[Anum_duckdb_secret_r2_account_id - 1]);

		duckdbSecrets.push_back(secret);
	}

	systable_endscan(scan);
	table_close(duckdbSecretRelation, NoLock);
	return duckdbSecrets;
}

std::vector<DuckdbExension>
ReadDuckdbExtensions() {
	HeapTuple tuple = NULL;
	Oid duckdbExtensionRelationId = duckdbExtensionsRelationId();
	Relation duckdbExtensionRelation = table_open(duckdbExtensionRelationId, AccessShareLock);
	SysScanDescData *scan =
	    systable_beginscan(duckdbExtensionRelation, InvalidOid, false, GetActiveSnapshot(), 0, NULL);
	std::vector<DuckdbExension> duckdbExtension;

	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		Datum datumArray[Natts_duckdb_secret];
		bool isNullArray[Natts_duckdb_secret];

		heap_deform_tuple(tuple, RelationGetDescr(duckdbExtensionRelation), datumArray, isNullArray);
		DuckdbExension secret;

		secret.name = duckdbDatumToString(datumArray[Anum_duckdb_extension_name - 1]);
		secret.enabled = DatumGetBool(datumArray[Anum_duckdb_extension_enable - 1]);
		duckdbExtension.push_back(secret);
	}

	systable_endscan(scan);
	table_close(duckdbExtensionRelation, NoLock);
	return duckdbExtension;
}

static bool
duckdbInstallExtension(Datum name) {
	auto db = DuckdbOpenDatabase();
	auto connection = duckdb::make_uniq<duckdb::Connection>(*db);
	auto &context = *connection->context;

	auto extensionName = duckdbDatumToString(name);

	StringInfo installExtensionCommand = makeStringInfo();
	appendStringInfo(installExtensionCommand, "INSTALL %s;", extensionName.c_str());

	auto res = context.Query(installExtensionCommand->data, false);

	pfree(installExtensionCommand->data);

	if (res->HasError()) {
		elog(WARNING, "(duckdb_install_extension) %s", res->GetError().c_str());
		return false;
	}

	bool nulls[Natts_duckdb_extension] = {0};
	Datum values[Natts_duckdb_extension] = {0};

	values[Anum_duckdb_extension_name - 1] = name;
	values[Anum_duckdb_extension_enable - 1] = 1;

	/* create heap tuple and insert into catalog table */
	Relation duckdbExtensionRelation = relation_open(duckdbExtensionsRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(duckdbExtensionRelation);

	/* inserting extension record */
	HeapTuple newTuple = heap_form_tuple(tupleDescriptor, values, nulls);
	CatalogTupleInsert(duckdbExtensionRelation, newTuple);

	CommandCounterIncrement();
	relation_close(duckdbExtensionRelation, RowExclusiveLock);

	return true;
}

} // namespace pgduckdb

extern "C" {

PG_FUNCTION_INFO_V1(install_extension);
Datum
install_extension(PG_FUNCTION_ARGS) {
	Datum extensionName = PG_GETARG_DATUM(0);
	bool result = pgduckdb::duckdbInstallExtension(extensionName);
	PG_RETURN_BOOL(result);
}

} // extern "C"
