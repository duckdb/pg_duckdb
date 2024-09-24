#include "pgduckdb/bgw/main.hpp"
#include "pgduckdb/bgw/messages.hpp"
#include "pgduckdb/bgw/network.hpp"
#include "pgduckdb/bgw/utils.hpp"
#include "duckdb.hpp"

namespace pgduckdb {

class PGDuckDBBGW {
public:
	PGDuckDBBGW(const int _fd, duckdb::DuckDB &_db) : network(_fd), db(_db) {
	}

	void
	RunQuery() {
		auto request = network.Read<PGDuckDBRunQuery>();
		// Do query DuckDB
		auto result = GetConnection().Query(request->GetQuery(), false);
		WriteQueryResult(std::move(result));
	}

	void
	SendQuery() {
		auto request = network.Read<PGDuckDBQuery>();
		// Do query DuckDB
		auto result = GetConnection().SendQuery(request->GetQuery());
		WriteQueryResult(std::move(result));
	}

	void
	PrepareQuery() {
		auto request = network.Read<PGDuckDBPrepareQuery>();
		auto result = GetConnection().Prepare(request->GetQuery());

		const auto result_id = GetNextResultId();
		network.Write(PGDuckDBPreparedQueryResult(result_id, result.get()));

		if (!result->HasError()) {
			prepared_statements[result_id] = std::move(result);
		}
	}

	void
	PreparedQueryMakePending() {
		const auto id = network.ReadId();

		const auto &ps = prepared_statements.find(id);
		if (ps == prepared_statements.end()) {
			auto err = "Could not find prepared query: " + std::to_string(id);
			network.Write(PGDuckDBPreparedQueryExecutionResult(err));
			return;
		}

		auto pending = ps->second->PendingQuery();
		if (DoExecuteTask(id, pending.get())) {
			pending_results[id] = std::move(pending);
		}
	}

	void
	PreparedQueryExecuteTask() {
		const auto id = network.ReadId();

		const auto &pending_pair = pending_results.find(id);
		if (pending_pair == pending_results.end()) {
			auto err = "Could not find pending query: " + std::to_string(id);
			network.Write(PGDuckDBPreparedQueryExecutionResult(err));
		} else {
			DoExecuteTask(id, pending_pair->second.get());
		}
	}

	void
	PrepareQueryExecute() {
		const auto id = network.ReadId();

		const auto &ps = prepared_statements.find(id);
		if (ps == prepared_statements.end()) {
			// TODO - better error handling (put it in result)
			throw std::runtime_error("Could not find prepared query: " + std::to_string(id));
		}

		auto res = ps->second->Execute();
		network.Write(PGDuckDBQueryResult(id, res.get()));
	}

	void
	GetNextChunk() {
		auto id = network.ReadId();
		const auto &result = results.find(id);

		if (result == results.end()) {
			// TODO - better error handling (put it in result)
			throw std::runtime_error("Could not find result: " + std::to_string(id));
		}

		network.Write(PGDuckDBQueryResult(id, result->second.get()));
	}

	void
	Close() {
		network.Close();
	}

	PGDuckDBMessageType
	ReadType() const {
		return network.ReadType();
	}

	void
	Interrupt() {
		assert(connection);

		// Send an interrupt
		connection->Interrupt();
		auto &executor = duckdb::Executor::Get(*connection->context);

		// Wait for all tasks to terminate
		executor.CancelTasks();

		// Ack
		network.Write(PGDuckDBInterrupt());
	}

private:
	bool
	DoExecuteTask(uint64_t id, duckdb::PendingQueryResult *pending) {
		if (pending->HasError()) {
			network.Write(PGDuckDBPreparedQueryExecutionResult(pending->GetError()));
			return false;
		}

		auto execution_result = pending->ExecuteTask();
		if (execution_result == duckdb::PendingExecutionResult::EXECUTION_ERROR) {
			network.Write(PGDuckDBPreparedQueryExecutionResult(pending->GetError()));
			return false;
		}

		auto res = PGDuckDBPreparedQueryExecutionResult(execution_result, -1);
		if (duckdb::PendingQueryResult::IsResultReady(execution_result)) {
			auto query_result = pending->Execute();
			res.SetColumnCount(query_result->ColumnCount());
			results[id] = std::move(query_result);
		}

		network.Write(res);
		return true;
	}

	template <typename T>
	void
	WriteQueryResult(duckdb::unique_ptr<T> result) {
		const auto result_id = GetNextResultId();
		network.Write(PGDuckDBQueryResult(result_id, result.get()));

		// Store result if successful
		// TODO - clear (manually & automatically)
		if (!result->HasError()) {
			results[result_id] = std::move(result);
		}
	}

	uint64_t
	GetNextResultId() {
		return next_result_id++;
	}

	duckdb::Connection &
	GetConnection() {
		if (!connection) {
			connection = duckdb::make_uniq<duckdb::Connection>(db);
		}

		return *connection;
	}

	const Network network;
	duckdb::DuckDB &db;
	duckdb::unique_ptr<duckdb::Connection> connection;
	uint64_t next_result_id;

	std::map<uint64_t, duckdb::unique_ptr<duckdb::QueryResult>> results;

	std::map<uint64_t, duckdb::unique_ptr<duckdb::PreparedStatement>> prepared_statements;

	std::map<uint64_t, duckdb::unique_ptr<duckdb::PendingQueryResult>> pending_results;
};

void
OnConnection(const int client_fd, duckdb::DuckDB &db) {
	PGDuckDBBGW bgw(client_fd, db);
	while (true) {
		PGDuckDBMessageType type = bgw.ReadType();
		// printf("SERVER: Received message type: %d\n", (int)type);
		switch (type) {
		case PGDuckDBMessageType::RUN_QUERY:
			bgw.RunQuery();
			continue;
		case PGDuckDBMessageType::SEND_QUERY:
			bgw.SendQuery();
			continue;
		case PGDuckDBMessageType::PREPARE_QUERY:
			bgw.PrepareQuery();
			continue;
		case PGDuckDBMessageType::PREPARED_QUERY_EXECUTE:
			bgw.PrepareQueryExecute();
			continue;
		case PGDuckDBMessageType::PREPARED_QUERY_MAKE_PENDING:
			bgw.PreparedQueryMakePending();
			continue;
		case PGDuckDBMessageType::PREPARED_QUERY_EXECUTE_TASK:
			bgw.PreparedQueryExecuteTask();
			continue;
		case PGDuckDBMessageType::GET_NEXT_CHUNK:
			bgw.GetNextChunk();
			continue;
		case PGDuckDBMessageType::INTERRUPT:
			bgw.Interrupt();
			continue;
		case PGDuckDBMessageType::CLOSE_CONNECTION:
			bgw.Close();
			return;
		case PGDuckDBMessageType::QUERY_RESULT:
		case PGDuckDBMessageType::PREPARED_QUERY_RESULT:
		case PGDuckDBMessageType::PREPARED_QUERY_EXECUTION_RESULT:
		case PGDuckDBMessageType::SENTINEL:
			break;
		}

		throw std::runtime_error("Invalid message type: <TODO>");
	}
}

} // namespace pgduckdb
