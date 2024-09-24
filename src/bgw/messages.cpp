#include "pgduckdb/bgw/messages.hpp"

namespace pgduckdb {

/* PGDuckDBQuery */
PGDuckDBMessageType PGDuckDBQuery::type = PGDuckDBMessageType::SEND_QUERY;

void
PGDuckDBQuery::Serialize(duckdb::Serializer &serializer) const {
	serializer.WriteProperty(1, "query", query);
}

duckdb::unique_ptr<PGDuckDBQuery>
PGDuckDBQuery::Deserialize(duckdb::Deserializer &deserializer) {
	auto query = deserializer.ReadProperty<std::string>(1, "query");
	return duckdb::make_uniq<PGDuckDBQuery>(query);
}

/* PGDuckDBRunQuery */
PGDuckDBMessageType PGDuckDBRunQuery::type = PGDuckDBMessageType::RUN_QUERY;

void
PGDuckDBRunQuery::Serialize(duckdb::Serializer &serializer) const {
	serializer.WriteProperty(1, "query", query);
}

duckdb::unique_ptr<PGDuckDBRunQuery>
PGDuckDBRunQuery::Deserialize(duckdb::Deserializer &deserializer) {
	auto query = deserializer.ReadProperty<std::string>(1, "query");
	return duckdb::make_uniq<PGDuckDBRunQuery>(query);
}

/* PGDuckDBQueryResult */

PGDuckDBMessageType PGDuckDBQueryResult::type = PGDuckDBMessageType::QUERY_RESULT;

std::string
PGDuckDBQueryResult::ToString() {
	return chunk ? chunk->ToString() : error->Message();
}

void
PGDuckDBQueryResult::Serialize(duckdb::Serializer &serializer) const {
	const bool success = !result->HasError();
	serializer.WriteProperty(1, "success", success);
	if (success) {
		serializer.WriteProperty(200, "id", id);

		auto chunk = result->Fetch();
		if (chunk) {
			serializer.WriteProperty(201, "has_chunk", true);
			chunk->Serialize(serializer);
		} else {
			serializer.WriteProperty(201, "has_chunk", false);
		}
	} else {
		auto &err = result->GetErrorObject();
		serializer.WriteProperty(301, "error_type", err.Type());
		serializer.WriteProperty(302, "error_message", err.RawMessage());
	}
}

duckdb::unique_ptr<PGDuckDBQueryResult>
PGDuckDBQueryResult::Deserialize(duckdb::Deserializer &deserializer) {
	auto result = duckdb::make_uniq<PGDuckDBQueryResult>();
	bool success = deserializer.ReadProperty<bool>(1, "success");
	if (success) {
		result->id = deserializer.ReadProperty<uint64_t>(200, "id");
		result->chunk = duckdb::make_uniq<duckdb::DataChunk>();

		const bool has_chunk = deserializer.ReadProperty<uint64_t>(201, "has_chunk");
		if (has_chunk) {
			result->chunk->Deserialize(deserializer);
		}
	} else {
		auto error_type = deserializer.ReadProperty<duckdb::ExceptionType>(301, "error_type");
		auto error_message = deserializer.ReadProperty<std::string>(302, "error_message");
		result->error = duckdb::make_uniq<duckdb::ErrorData>(error_type, error_message);
	}

	return result;
}

/* PGDuckDBPrepareQuery */

PGDuckDBMessageType PGDuckDBPrepareQuery::type = PGDuckDBMessageType::PREPARE_QUERY;

void
PGDuckDBPrepareQuery::Serialize(duckdb::Serializer &serializer) const {
	serializer.WriteProperty(101, "query", query);
}

duckdb::unique_ptr<PGDuckDBPrepareQuery>
PGDuckDBPrepareQuery::Deserialize(duckdb::Deserializer &deserializer) {
	auto query = deserializer.ReadProperty<std::string>(101, "query");
	return duckdb::make_uniq<PGDuckDBPrepareQuery>(query);
}

/* PGDuckDBPreparedQueryResult */

PGDuckDBMessageType PGDuckDBPreparedQueryResult::type = PGDuckDBMessageType::PREPARED_QUERY_RESULT;

void
PGDuckDBPreparedQueryResult::Serialize(duckdb::Serializer &serializer) const {
	const bool success = !ps->HasError();
	serializer.WriteProperty(101, "success", success);
	if (success) {
		serializer.WriteProperty(102, "id", id);
		serializer.WriteProperty(103, "names", ps->GetNames());
		serializer.WriteProperty(104, "types", ps->GetTypes());
	} else {
		auto &err = ps->GetErrorObject();
		serializer.WriteProperty(301, "error_type", err.Type());
		serializer.WriteProperty(302, "error_message", err.RawMessage());
	}
}

duckdb::unique_ptr<PGDuckDBPreparedQueryResult>
PGDuckDBPreparedQueryResult::Deserialize(duckdb::Deserializer &deserializer) {
	auto result = duckdb::make_uniq<PGDuckDBPreparedQueryResult>();
	bool success = deserializer.ReadProperty<bool>(101, "success");
	if (success) {
		result->id = deserializer.ReadProperty<uint64_t>(102, "id");
		result->names = deserializer.ReadProperty<duckdb::vector<std::string>>(103, "names");
		result->types = deserializer.ReadProperty<duckdb::vector<duckdb::LogicalType>>(104, "types");
	} else {
		auto error_type = deserializer.ReadProperty<duckdb::ExceptionType>(301, "error_type");
		auto error_message = deserializer.ReadProperty<std::string>(302, "error_message");
		result->error = duckdb::make_uniq<duckdb::ErrorData>(error_type, error_message);
	}

	return result;
}

PGDuckDBMessageType PGDuckDBInterrupt::type = PGDuckDBMessageType::INTERRUPT;

PGDuckDBMessageType PGDuckDBPreparedQueryExecutionResult::type = PGDuckDBMessageType::PREPARED_QUERY_EXECUTION_RESULT;

PGDuckDBPreparedQueryExecutionResult::PGDuckDBPreparedQueryExecutionResult(const std::string &_error)
    : has_error(true), execution_result(duckdb::PendingExecutionResult::EXECUTION_ERROR), column_count(-1),
      error(_error) {
}

PGDuckDBPreparedQueryExecutionResult::PGDuckDBPreparedQueryExecutionResult(duckdb::PendingExecutionResult _er,
                                                                           duckdb::idx_t _cc)
    : has_error(false), execution_result(_er), column_count(_cc) {
}

void
PGDuckDBPreparedQueryExecutionResult::Serialize(duckdb::Serializer &serializer) const {
	serializer.WriteProperty(101, "has_error", has_error);
	if (has_error) {
		serializer.WriteProperty(102, "error", error);
	} else {
		serializer.WriteProperty(103, "execution_result", execution_result);
		serializer.WriteProperty(104, "column_count", column_count);
	}
}

duckdb::unique_ptr<PGDuckDBPreparedQueryExecutionResult>
PGDuckDBPreparedQueryExecutionResult::Deserialize(duckdb::Deserializer &deserializer) {
	const bool has_error = deserializer.ReadProperty<bool>(101, "has_error");
	if (has_error) {
		const auto err = deserializer.ReadProperty<std::string>(102, "error");
		return duckdb::make_uniq<PGDuckDBPreparedQueryExecutionResult>(err);
	} else {
		const auto er = deserializer.ReadProperty<duckdb::PendingExecutionResult>(103, "execution_result");
		const auto cc = deserializer.ReadProperty<duckdb::idx_t>(104, "column_count");
		return duckdb::make_uniq<PGDuckDBPreparedQueryExecutionResult>(er, cc);
	}
}

} // namespace pgduckdb