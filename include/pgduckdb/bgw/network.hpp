#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/unique_ptr.hpp>

namespace pgduckdb {

enum class PGDuckDBMessageType : uint16_t {
	// Requests
	SEND_QUERY = 0,
	RUN_QUERY,
	PREPARE_QUERY,
	PREPARED_QUERY_EXECUTE,
	PREPARED_QUERY_MAKE_PENDING,
	PREPARED_QUERY_EXECUTE_TASK,
	GET_NEXT_CHUNK,

	// Responses
	QUERY_RESULT,
	PREPARED_QUERY_RESULT,
	PREPARED_QUERY_EXECUTION_RESULT,
	PREPARED_QUERY_MAKE_PENDING_ACK,

	// Admin
	CLOSE_CONNECTION,
	INTERRUPT,
	SENTINEL
};

struct Network {
	Network(const int _fd);

	template <class T>
	void
	Write(const T &serializable) const {
		duckdb::MemoryStream stream;
		duckdb::BinarySerializer::Serialize(serializable, stream);

		WriteValue(T::type);

		const uint64_t length = stream.GetPosition();
		WriteValue(length);
		WriteBuffer(stream.GetData(), length);
	}

	void Write(PGDuckDBMessageType t, uint64_t id) const;
	void Write(PGDuckDBMessageType t) const;

	template <class T>
	duckdb::unique_ptr<T>
	Read() const {
		uint64_t length = ReadValue<uint64_t>();

		// DuckDB requires the length to be a power of 2
		// Ref. https://stackoverflow.com/a/10143264/667433
		uint64_t length_aligned = 1ULL << (sizeof(uint64_t) * 8 - __builtin_clzll(length));
		duckdb::MemoryStream stream(length_aligned);
		ReadBuffer(stream.GetData(), length);

		return duckdb::BinaryDeserializer::Deserialize<T>(stream);
	}

	PGDuckDBMessageType ReadType() const;

	uint64_t ReadId() const;

	void Close() const;

private:
	template <typename T>
	T
	ReadValue() const {
		T data;
		ReadBuffer((uint8_t *)&data, sizeof(T));
		return data;
	}

	void ReadBuffer(uint8_t *, size_t) const;

	template <typename T>
	void
	WriteValue(const T data) const {
		WriteBuffer((const duckdb::data_ptr_t)&data, sizeof(T));
	}

	void WriteBuffer(uint8_t *, size_t) const;

	const int fd;
};

} // namespace pgduckdb