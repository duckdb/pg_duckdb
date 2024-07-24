
#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/genam.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
}

#include "quack/quack_options.hpp"
#include "quack/quack_duckdb.hpp"

namespace quack {

static Oid
quackGetNamespace(void) {
	return get_namespace_oid("quack", false);
}

static Oid
quackSecretsRelationId(void) {
	return get_relname_relid("secrets", quackGetNamespace());
}

static Oid
quackExtensionsRelationId(void) {
	return get_relname_relid("extensions", quackGetNamespace());
}

static std::string
quackDatumToString(Datum datum) {
	std::string columnValue;
	text *cloudType = DatumGetTextPP(datum);
	columnValue = VARDATA_ANY(cloudType);
	columnValue.resize(VARSIZE_ANY_EXHDR(cloudType));
	return columnValue;
}

std::vector<QuackSecret>
read_quack_secrets() {
	HeapTuple tuple = NULL;
	Oid quackSecretRelationId = quackSecretsRelationId();
	Relation quackSecretRelation = table_open(quackSecretRelationId, AccessShareLock);
	SysScanDescData *scan = systable_beginscan(quackSecretRelation, InvalidOid, false, GetActiveSnapshot(), 0, NULL);
	std::vector<QuackSecret> quackSecrets;

	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		Datum datumArray[Natts_quack_secret];
		bool isNullArray[Natts_quack_secret];

		heap_deform_tuple(tuple, RelationGetDescr(quackSecretRelation), datumArray, isNullArray);
		QuackSecret secret;

		secret.type = quackDatumToString(datumArray[Anum_quack_secret_type - 1]);
		secret.id = quackDatumToString(datumArray[Anum_quack_secret_id - 1]);
		secret.secret = quackDatumToString(datumArray[Anum_quack_secret_secret - 1]);

		if (!isNullArray[Anum_quack_secret_region - 1])
			secret.region = quackDatumToString(datumArray[Anum_quack_secret_region - 1]);

		if (!isNullArray[Anum_quack_secret_endpoint - 1])
			secret.endpoint = quackDatumToString(datumArray[Anum_quack_secret_endpoint - 1]);

		if (!isNullArray[Anum_quack_secret_r2_account_id - 1])
			secret.endpoint = quackDatumToString(datumArray[Anum_quack_secret_r2_account_id - 1]);

		quackSecrets.push_back(secret);
	}

	systable_endscan(scan);
	table_close(quackSecretRelation, NoLock);
	return quackSecrets;
}

std::vector<QuackExension>
read_quack_extensions() {
	HeapTuple tuple = NULL;
	Oid quackExtensionRelationId = quackExtensionsRelationId();
	Relation quackExtensionRelation = table_open(quackExtensionRelationId, AccessShareLock);
	SysScanDescData *scan = systable_beginscan(quackExtensionRelation, InvalidOid, false, GetActiveSnapshot(), 0, NULL);
	std::vector<QuackExension> quackExtension;

	while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
		Datum datumArray[Natts_quack_secret];
		bool isNullArray[Natts_quack_secret];

		heap_deform_tuple(tuple, RelationGetDescr(quackExtensionRelation), datumArray, isNullArray);
		QuackExension secret;

		secret.name = quackDatumToString(datumArray[Anum_quack_extension_name - 1]);
		secret.enabled = DatumGetBool(datumArray[Anum_quack_extension_enable - 1]);
		quackExtension.push_back(secret);
	}

	systable_endscan(scan);
	table_close(quackExtensionRelation, NoLock);
	return quackExtension;
}

static bool
quackInstallExtension(Datum name) {
	auto db = quack_open_database();
	auto connection = duckdb::make_uniq<duckdb::Connection>(*db);
	auto &context = *connection->context;

	auto extensionName = quackDatumToString(name);

	StringInfo installExtensionCommand = makeStringInfo();
	appendStringInfo(installExtensionCommand, "INSTALL %s;", extensionName.c_str());

	auto res = context.Query(installExtensionCommand->data, false);

	pfree(installExtensionCommand->data);

	if (res->HasError()) {
		elog(WARNING, "(quack_install_extension) %s", res->GetError().c_str());
		return false;
	}

	bool nulls[Natts_quack_extension] = {0};
	Datum values[Natts_quack_extension] = {0};

	values[Anum_quack_extension_name - 1] = name;
	values[Anum_quack_extension_enable - 1] = 1;

	/* create heap tuple and insert into catalog table */
	Relation quackExtensionRelation = relation_open(quackExtensionsRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(quackExtensionRelation);

	/* inserting extension record */
	HeapTuple newTuple = heap_form_tuple(tupleDescriptor, values, nulls);
	CatalogTupleInsert(quackExtensionRelation, newTuple);

	CommandCounterIncrement();
	relation_close(quackExtensionRelation, RowExclusiveLock);

	return true;
}

static bool
quack_run_query(const char *query) {
	auto db = quack::quack_open_database();
	auto connection = duckdb::make_uniq<duckdb::Connection>(*db);
	auto &context = *connection->context;

	auto res = context.Query("FORCE INSTALL "
	                         "'/home/jelte/work/pg_quack_internal/third_party/demo-extension/build/release/extension/"
	                         "demo_in_pg/demo_in_pg.duckdb_extension';",
	                         false);

	if (res->HasError()) {
		elog(WARNING, "(quack_install_extension) %s", res->GetError().c_str());
		return false;
	}
	res = context.Query("LOAD demo_in_pg", false);

	if (res->HasError()) {
		elog(WARNING, "(quack_install_extension) %s", res->GetError().c_str());
		return false;
	}
	res = context.Query(query, false);

	if (res->HasError()) {
		elog(WARNING, "failed to run query: %s", res->GetError().c_str());
		return false;
	}
	elog(NOTICE, "result: %s", res->ToString().c_str());
	return true;
}

} // namespace quack

extern "C" {

PG_FUNCTION_INFO_V1(install_extension);
Datum
install_extension(PG_FUNCTION_ARGS) {
	Datum extensionName = PG_GETARG_DATUM(0);
	bool result = quack::quackInstallExtension(extensionName);
	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(run);
Datum
run(PG_FUNCTION_ARGS) {
	const char *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bool result = quack::quack_run_query(query);
	PG_RETURN_BOOL(result);
}

} // extern "C"
