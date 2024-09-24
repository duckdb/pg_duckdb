#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

extern "C" {
#include "postgres.h"
}

#include <unistd.h>
#include "pgduckdb/bgw/utils.hpp"
#include "pgduckdb/bgw/client.hpp"
#include "pgduckdb/bgw/messages.hpp"
#include "pgduckdb/bgw/tests/main.hpp"

using namespace pgduckdb;

void
AssertTrue(bool condition, const char *message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

void
AssertEquals(const std::string &actual, const std::string &expected, const std::string &what = "") {
	if (expected != actual) {
		throw std::runtime_error("Expected: '" + expected + "' but got: '" + actual + "'");
	}
}

template <typename T, typename U>
void
AssertEquals(const T &actual, const U &expected, const std::string &what) {
	if (expected != actual) {
		throw std::runtime_error("Expected " + what + " to equal '" + std::to_string(expected) + "' but got: '" +
		                         std::to_string(actual) + "'");
	}
}

template <typename T, typename U>
void
AssertEquals(const duckdb::vector<T> &actual, const duckdb::vector<U> &expected, const std::string &what) {
	if (expected.size() != actual.size()) {
		throw std::runtime_error("Expected " + what + " to have a size of '" + std::to_string(expected.size()) +
		                         "' but got: '" + std::to_string(actual.size()) + "'");
	}

	for (size_t i = 0; i < expected.size(); ++i) {
		AssertEquals(actual[i], expected[i], what + " at " + std::to_string(i));
	}
}

void
AssertContains(const std::string &actual, const std::string &expected) {
	if (actual.find(expected) == std::string::npos) {
		throw std::runtime_error("Expected to find: '" + expected + "' but got: '" + actual + "'");
	}
}

namespace pgduckdb_tests {

std::vector<std::pair<std::string, std::function<void()>>> PGDUCKDB_TESTS = {
    {"Basic query with BGW",
     []() {
	     auto client = PGDuckDBBgwClient();
	     client.Connect();

	     auto query = PGDuckDBQuery("SELECT 42");
	     auto query_result = client.SubmitQuery(query);

	     AssertTrue(query_result->IsSuccess(), "Query failed");
	     AssertEquals(query_result->ToString(), "Chunk - [1 Columns]\n- FLAT INTEGER: 1 = [ 42]\n");
     }},
    {"Failing query with BGW",
     []() {
	     auto client = PGDuckDBBgwClient();
	     client.Connect();

	     auto query = PGDuckDBQuery("SELECT foo");
	     auto query_result = client.SubmitQuery(query);

	     AssertTrue(!query_result->IsSuccess(), "Query should have failed");
	     AssertContains(query_result->ToString(), "Binder Error: Referenced column \"foo\" not found in FROM clause!");
     }},

    {"Run query with BGW",
     []() {
	     auto client = PGDuckDBBgwClient();
	     client.Connect();

	     auto query_result = client.RunQuery("SELECT 42");

	     AssertTrue(query_result->IsSuccess(), "Query failed");
	     AssertEquals(query_result->ToString(), "Chunk - [1 Columns]\n- FLAT INTEGER: 1 = [ 42]\n");
     }},

    {"Fetching multiple chunks",
     []() {
	     auto client = PGDuckDBBgwClient();
	     client.Connect();

	     auto query = PGDuckDBQuery("SELECT * FROM generate_series(0, 1e5::int);");
	     auto query_result = client.SubmitQuery(query);

	     AssertTrue(query_result->IsSuccess(), "Query failed");
	     const auto id = query_result->GetId();
	     {
		     const auto &chunk = query_result->GetChunk();
		     AssertEquals(chunk.size(), DEFAULT_STANDARD_VECTOR_SIZE, "Chunk size");
		     AssertEquals(chunk.ColumnCount(), 1, "Column count");

		     for (uint32_t i = 0; i < 5; ++i) {
			     AssertEquals(chunk.GetValue(0, i).GetValue<uint32_t>(), i, "Value #" + std::to_string(i));
		     }
	     }

	     {
		     auto next_result = client.FetchNextChunk(id);
		     const auto &chunk = next_result->GetChunk();
		     AssertEquals(chunk.size(), DEFAULT_STANDARD_VECTOR_SIZE, "Chunk size");
		     AssertEquals(chunk.ColumnCount(), 1, "Column count");

		     for (uint32_t i = 0; i < 5; ++i) {
			     AssertEquals(chunk.GetValue(0, i).GetValue<uint32_t>(), i + DEFAULT_STANDARD_VECTOR_SIZE,
			                  "Value #" + std::to_string(i));
		     }
	     }
     }},

    {"Prepare query successfully",
     []() {
	     auto client = PGDuckDBBgwClient();
	     client.Connect();

	     auto query = PGDuckDBPrepareQuery("SELECT 42 as forty_two");

	     auto query_result = client.PrepareQuery(query);

	     AssertTrue(query_result->IsSuccess(), "Query failed");
	     AssertEquals(query_result->GetNames(), duckdb::vector<std::string> {"forty_two"}, "names");
	     AssertEquals(query_result->GetTypes().size(), 1, "types size");
	     AssertEquals((int)query_result->GetTypes()[0].id(), (int)duckdb::LogicalTypeId::INTEGER, "type");
     }},

    {"Failed to prepare query",
     []() {
	     auto client = PGDuckDBBgwClient();
	     client.Connect();

	     auto query = PGDuckDBPrepareQuery("SELECT foo");
	     auto query_result = client.PrepareQuery(query);

	     AssertTrue(!query_result->IsSuccess(), "Query failed");
	     AssertContains(query_result->GetError().Message(),
	                    "Binder Error: Referenced column \"foo\" not found in FROM clause!");
     }},

    // from generate_series(0, 9);
    // {"Trivial test", []() {
    //   // Foo foo_in;
    //   // foo_in.a = 42;
    //   // // foo_in.bar = make_uniq<Bar>();
    //   // // foo_in.bar->b = 43;
    //   // foo_in.c = 44;

    //   // duckdb::MemoryStream stream;
    //   // duckdb::BinarySerializer::Serialize(foo_in, stream);
    //   // const auto pos = stream.GetPosition();
    //   // stream.Rewind();
    //   // elog(DEBUG1, "Hey this test is going to work!");
    //   // elog(INFO, "Position: %llu", pos);
    // }},
    // {"Test failing with a std::exception", []() {
    //   throw std::runtime_error("**Test 4 failed (std)**");
    // }},
    // {"Test failing with a PG exception", []() {
    //   elog(ERROR, "** Test 5 failed (pg) **");
    // }},
};
} // namespace pgduckdb_tests