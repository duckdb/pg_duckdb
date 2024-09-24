#pragma once

#include "pgduckdb/bgw/network.hpp"

#include <string>

namespace pgduckdb {

class PGDuckDBQuery {

public:
	static PGDuckDBMessageType type;

	PGDuckDBQuery(const std::string &_query) : query(_query) {};

	const std::string &
	GetQuery() const {
		return query;
	}

	void Serialize(duckdb::Serializer &serializer) const;

	static duckdb::unique_ptr<PGDuckDBQuery> Deserialize(duckdb::Deserializer &deserializer);

private:
	std::string query;
};

class PGDuckDBRunQuery {

public:
	static PGDuckDBMessageType type;

	PGDuckDBRunQuery(const std::string &_query) : query(_query) {};

	const std::string &
	GetQuery() const {
		return query;
	}

	void Serialize(duckdb::Serializer &serializer) const;

	static duckdb::unique_ptr<PGDuckDBRunQuery> Deserialize(duckdb::Deserializer &deserializer);

private:
	std::string query;
};

class PGDuckDBQueryResult {
public:
	PGDuckDBQueryResult(uint64_t _id, duckdb::QueryResult *_result) : id(_id), result(_result) {};

	PGDuckDBQueryResult() {}; // Used when Client reads the result

	static PGDuckDBMessageType type;

	void Serialize(duckdb::Serializer &serializer) const;

	static duckdb::unique_ptr<PGDuckDBQueryResult> Deserialize(duckdb::Deserializer &deserializer);

	std::string ToString();

	inline bool
	IsSuccess() const {
		return !error;
	}

	inline bool
	HasError() const {
		return !!error;
	}

	inline uint64_t
	GetId() const {
		return id;
	}

	inline const duckdb::DataChunk &
	GetChunk() const {
		return *chunk;
	}

	inline duckdb::unique_ptr<duckdb::DataChunk>
	MoveChunk() {
		return std::move(chunk);
	}

	inline const duckdb::ErrorData &
	GetError() const {
		return *error;
	}

private:
	// Used during serialization & deserialization
	uint64_t id;

	// Used when BGW serialize the result
	duckdb::QueryResult *result;

	// Used when client reads the result
	duckdb::unique_ptr<duckdb::DataChunk> chunk;
	duckdb::unique_ptr<duckdb::ErrorData> error;
};

class PGDuckDBPrepareQuery {

public:
	static PGDuckDBMessageType type;

	PGDuckDBPrepareQuery(const std::string &_query) : query(_query) {};

	const std::string &
	GetQuery() const {
		return query;
	}

	void Serialize(duckdb::Serializer &serializer) const;

	static duckdb::unique_ptr<PGDuckDBPrepareQuery> Deserialize(duckdb::Deserializer &deserializer);

private:
	std::string query;
};

class PGDuckDBInterrupt {
public:
	static PGDuckDBMessageType type;

	void
	Serialize(duckdb::Serializer &serializer) const {
	}

	static void
	Deserialize(duckdb::Deserializer &deserializer) {
	}
};

class PGDuckDBPreparedQueryResult {

public:
	PGDuckDBPreparedQueryResult(uint64_t _id, duckdb::PreparedStatement *_ps) : id(_id), ps(_ps) {};

	PGDuckDBPreparedQueryResult() {}; // Used when Client reads the result

	static PGDuckDBMessageType type;

	void Serialize(duckdb::Serializer &serializer) const;

	static duckdb::unique_ptr<PGDuckDBPreparedQueryResult> Deserialize(duckdb::Deserializer &deserializer);

	inline uint64_t
	GetId() const {
		return id;
	}

	inline bool
	IsSuccess() const {
		return !error;
	}

	inline bool
	HasError() const {
		return !!error;
	}

	inline const duckdb::vector<std::string> &
	GetNames() const {
		return names;
	}

	inline const duckdb::vector<duckdb::LogicalType> &
	GetTypes() const {
		return types;
	}

	inline const duckdb::ErrorData &
	GetError() const {
		return *error;
	}

private:
	// Used during serialization & deserialization
	uint64_t id;

	// Used when BGW serialize the result
	duckdb::PreparedStatement *ps;

	// Used when client reads the result
	duckdb::unique_ptr<duckdb::ErrorData> error;

	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<std::string> names;
};

class PGDuckDBPreparedQueryExecutionResult {
public:
	static PGDuckDBMessageType type;

	PGDuckDBPreparedQueryExecutionResult() {};

	PGDuckDBPreparedQueryExecutionResult(duckdb::PendingExecutionResult, duckdb::idx_t); // deserialize

	// PGDuckDBPreparedQueryExecutionResult(duckdb::PendingExecutionResult); // success

	PGDuckDBPreparedQueryExecutionResult(const std::string &); // error

	void Serialize(duckdb::Serializer &serializer) const;

	static duckdb::unique_ptr<PGDuckDBPreparedQueryExecutionResult> Deserialize(duckdb::Deserializer &deserializer);

	inline duckdb::idx_t
	GetColumnCount() const {
		return column_count;
	}

	inline void
	SetColumnCount(duckdb::idx_t count) {
		column_count = count;
	}

	inline duckdb::PendingExecutionResult
	GetExecutionResult() const {
		return execution_result;
	}

	inline bool
	HasError() const {
		return has_error;
	}

	inline std::string
	GetError() const {
		assert(has_error);
		return error;
	}

private:
	bool has_error;
	duckdb::PendingExecutionResult execution_result;
	duckdb::idx_t column_count;
	std::string error;
};

} // namespace pgduckdb
