#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/bgw/utils.hpp"
#include "pgduckdb/bgw/client.hpp"
#include "pgduckdb/bgw/messages.hpp"
#include "pgduckdb/bgw/network.hpp"
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <string.h>

namespace pgduckdb {

void
PGDuckDBBgwClient::Connect() {
	int fd = socket(AF_LOCAL, SOCK_STREAM, 0);
	if (fd < 0) {
		pgduckdb::ELogError("connect_to_bgw", "socket");
	}

	struct sockaddr_un name;
	name.sun_family = AF_LOCAL;
	strncpy(name.sun_path, pgduckdb::PGDUCKDB_SOCKET_PATH, sizeof(name.sun_path));
	name.sun_path[sizeof(name.sun_path) - 1] = '\0';

	if (::connect(fd, (struct sockaddr *)&name, SUN_LEN(&name)) < 0) {
		pgduckdb::ELogError("connect_to_bgw", "connect");
	}

	network = duckdb::make_uniq<Network>(fd);
}

PGDuckDBBgwClient::~PGDuckDBBgwClient() {
	if (network) {
		network->Close();
	}
}

duckdb::unique_ptr<PGDuckDBQueryResult>
PGDuckDBBgwClient::SubmitQuery(PGDuckDBQuery &query) const {
	network->Write(query);
	auto t = network->ReadType();
	assert(t == PGDuckDBMessageType::QUERY_RESULT);
	return network->Read<PGDuckDBQueryResult>();
}

duckdb::unique_ptr<PGDuckDBQueryResult>
PGDuckDBBgwClient::RunQuery(const std::string &query) const {
	network->Write(PGDuckDBQuery(query));
	auto t = network->ReadType();
	assert(t == PGDuckDBMessageType::QUERY_RESULT);
	return network->Read<PGDuckDBQueryResult>();
}

duckdb::unique_ptr<PGDuckDBQueryResult>
PGDuckDBBgwClient::FetchNextChunk(uint64_t id) const {
	network->Write(PGDuckDBMessageType::GET_NEXT_CHUNK, id);
	auto t = network->ReadType();
	assert(t == PGDuckDBMessageType::QUERY_RESULT);
	return network->Read<PGDuckDBQueryResult>();
}

void
PGDuckDBBgwClient::Interrupt() const {
	network->Write(PGDuckDBMessageType::INTERRUPT);
	auto t = network->ReadType();
	assert(t == PGDuckDBMessageType::INTERRUPT);
}

duckdb::unique_ptr<PGDuckDBPreparedQueryExecutionResult>
PGDuckDBBgwClient::PreparedQueryExecuteTask(uint64_t id) const {
	network->Write(PGDuckDBMessageType::PREPARED_QUERY_EXECUTE_TASK, id);
	auto t = network->ReadType();
	assert(t == PGDuckDBMessageType::PREPARED_QUERY_EXECUTION_RESULT);
	return network->Read<PGDuckDBPreparedQueryExecutionResult>();
}

duckdb::unique_ptr<PGDuckDBQueryResult>
PGDuckDBBgwClient::PreparedQueryExecute(uint64_t id) const {
	network->Write(PGDuckDBMessageType::PREPARED_QUERY_EXECUTE, id);
	auto t = network->ReadType();
	assert(t == PGDuckDBMessageType::QUERY_RESULT);
	return network->Read<PGDuckDBQueryResult>();
}

duckdb::unique_ptr<PGDuckDBPreparedQueryExecutionResult>
PGDuckDBBgwClient::PrepareQueryMakePending(uint64_t id) const {
	network->Write(PGDuckDBMessageType::PREPARED_QUERY_MAKE_PENDING, id);
	auto t = network->ReadType();
	assert(t == PGDuckDBMessageType::PREPARED_QUERY_EXECUTION_RESULT);
	return network->Read<PGDuckDBPreparedQueryExecutionResult>();
}

duckdb::unique_ptr<PGDuckDBPreparedQueryResult>
PGDuckDBBgwClient::PrepareQuery(PGDuckDBPrepareQuery &query) const {
	network->Write(query);
	auto t = network->ReadType();
	assert(t == PGDuckDBMessageType::PREPARED_QUERY_RESULT);
	return network->Read<PGDuckDBPreparedQueryResult>();
}

PGDuckDBBgwClient &
PGDuckDBBgwClient::Get() {
	static duckdb::unique_ptr<PGDuckDBBgwClient> instance;

	if (!instance) {
		instance = duckdb::make_uniq<PGDuckDBBgwClient>();
		instance->Connect();
	}

	return *instance;
}

} // namespace pgduckdb