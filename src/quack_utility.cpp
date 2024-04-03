#include "duckdb.hpp"
#include "quack/quack.hpp"

extern "C" {

#include "postgres.h"

#include "miscadmin.h"

#include "catalog/pg_type.h"
#include "utils/fmgrprotos.h"

// DuckDB has date starting from 1/1/1970 while PG starts from 1/1/2000
#define QUACK_DUCK_DATE_OFFSET      10957
#define QUACK_DUCK_TIMESTAMP_OFFSET INT64CONST(10957) * USECS_PER_DAY

static StringInfo quack_database_path(Oid databaseOid) {
	StringInfo str = makeStringInfo();
	appendStringInfo(str, "%s/%u.duckdb", quack_data_dir, databaseOid);
	return str;
}

const char *quack_duckdb_type(Oid columnOid) {
	switch (columnOid) {
	case BOOLOID:
		return "BOOLEAN";
	case CHAROID:
		return "TINYINT";
	case INT2OID:
		return "SMALLINT";
	case INT4OID:
		return "INTEGER";
	case INT8OID:
		return "INT8";
	case BPCHAROID:
	case TEXTOID:
	case VARCHAROID:
		return "TEXT";
	case DATEOID:
		return "DATE";
	case TIMESTAMPOID:
		return "TIMESTAMP";
	default:
		elog(ERROR, "Unsuported quack type: %d", columnOid);
	}
}

} // extern "C"

namespace duckdb {

void quack_translate_value(TupleTableSlot *slot, Value &value, idx_t col) {
	Oid oid = slot->tts_tupleDescriptor->attrs[col].atttypid;

	switch (oid) {
	case BOOLOID:
		slot->tts_values[col] = value.GetValue<bool>();
		break;
	case CHAROID:
		slot->tts_values[col] = value.GetValue<int8_t>();
		break;
	case INT2OID:
		slot->tts_values[col] = value.GetValue<int16_t>();
		break;
	case INT4OID:
		slot->tts_values[col] = value.GetValue<int32_t>();
		break;
	case INT8OID:
		slot->tts_values[col] = value.GetValue<int64_t>();
		break;
	case BPCHAROID:
	case TEXTOID:
	case VARCHAROID: {
		auto str = value.GetValue<string>();
		auto varchar = str.c_str();
		auto varchar_len = str.size();

		text *result = (text *)palloc0(varchar_len + VARHDRSZ);
		SET_VARSIZE(result, varchar_len + VARHDRSZ);
		memcpy(VARDATA(result), varchar, varchar_len);
		slot->tts_values[col] = PointerGetDatum(result);
		// FIXME: this doesn't need to be freed, the string_t is owned by the chunk right?
		// duckdb_free(varchar);
		break;
	}
	case DATEOID: {
		date_t date = value.GetValue<date_t>();
		slot->tts_values[col] = date.days - QUACK_DUCK_DATE_OFFSET;
		break;
	}
	case TIMESTAMPOID: {
		dtime_t timestamp = value.GetValue<dtime_t>();
		slot->tts_values[col] = timestamp.micros - QUACK_DUCK_TIMESTAMP_OFFSET;
		break;
	}
	case FLOAT8OID:
	case NUMERICOID: {
		double result_double = value.GetValue<double>();
		slot->tts_tupleDescriptor->attrs[col].atttypid = FLOAT8OID;
		slot->tts_tupleDescriptor->attrs[col].attbyval = true;
		memcpy(&slot->tts_values[col], (char *)&result_double, sizeof(double));
		break;
	}
	default:
		elog(ERROR, "Unsuported quack type: %d", oid);
	}
}

void quack_execute_query(const char *query) {
	auto db = quack_open_database(MyDatabaseId, true);
	Connection connection(*db);

	std::string query_string = std::string(query);
	auto res = connection.Query(query_string);
	// FIME: res.HasError() ??
}

unique_ptr<Appender> quack_create_appender(Connection &connection, const char *tableName) {
	// FIXME: try-catch ?
	return make_uniq<Appender>(connection, "", std::string(tableName));
}

void quack_append_value(Appender &appender, Oid columnOid, Datum value) {
	switch (columnOid) {
	case BOOLOID:
		appender.Append<bool>(value);
		break;
	case CHAROID:
		appender.Append<int8_t>(value);
		break;
	case INT2OID:
		appender.Append<int16_t>(value);
		break;
	case INT4OID:
		appender.Append<int32_t>(value);
		break;
	case INT8OID:
		appender.Append<int64_t>(value);
		break;
	case BPCHAROID:
	case TEXTOID:
	case VARCHAROID: {
		const char *text = VARDATA_ANY(value);
		int len = VARSIZE_ANY_EXHDR(value);
		string_t str(text, len);
		appender.Append<string_t>(str);
		break;
	}
	case DATEOID: {
		date_t date(static_cast<int32_t>(value + QUACK_DUCK_DATE_OFFSET));
		appender.Append<date_t>(date);
		break;
	}
	case TIMESTAMPOID: {
		dtime_t timestamp(static_cast<int64_t>(value + QUACK_DUCK_TIMESTAMP_OFFSET));
		appender.Append<dtime_t>(timestamp);
		break;
	}
	default:
		elog(ERROR, "Unsuported quack type: %d", columnOid);
	}
}

unique_ptr<DuckDB> quack_open_database(Oid databaseOid, bool preserveInsertOrder) {
	/* Set lock for relation until transaction ends */
	DirectFunctionCall1(pg_advisory_xact_lock_int8, Int64GetDatum((int64)databaseOid));

	DBConfig config;
	config.SetOptionByName("preserve_insertion_order", Value::BOOLEAN(false));

	StringInfo database_path = quack_database_path(databaseOid);

	// FIXME: Does this need try-catch?
	return make_uniq<DuckDB>(database_path->data, &config);
}

unique_ptr<Connection> quack_open_connection(DuckDB database) {
	// FIXME try-catch ?
	return make_uniq<Connection>(database);
}

} // namespace duckdb
