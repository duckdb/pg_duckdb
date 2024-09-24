#pragma once

#include "pgduckdb/bgw/network.hpp"

namespace pgduckdb {

class PGDuckDBQuery;
class PGDuckDBQueryResult;
class PGDuckDBPrepareQuery;
class PGDuckDBPreparedQueryResult;
class PGDuckDBPreparedQueryExecutionResult;

class PGDuckDBBgwClient {
public:
	~PGDuckDBBgwClient();

	void Connect();

	duckdb::unique_ptr<PGDuckDBQueryResult> RunQuery(const std::string &) const;

	duckdb::unique_ptr<PGDuckDBQueryResult> SubmitQuery(PGDuckDBQuery &) const;

	duckdb::unique_ptr<PGDuckDBPreparedQueryResult> PrepareQuery(PGDuckDBPrepareQuery &) const;

	duckdb::unique_ptr<PGDuckDBPreparedQueryExecutionResult> PrepareQueryMakePending(uint64_t) const;

	duckdb::unique_ptr<PGDuckDBPreparedQueryExecutionResult> PreparedQueryExecuteTask(uint64_t) const;

	duckdb::unique_ptr<PGDuckDBQueryResult> PreparedQueryExecute(uint64_t) const;

	duckdb::unique_ptr<PGDuckDBQueryResult> FetchNextChunk(uint64_t) const;

	void Interrupt() const;

	static PGDuckDBBgwClient &Get();

private:
	duckdb::unique_ptr<Network> network;
};

} // namespace pgduckdb
