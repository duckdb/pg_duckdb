#include "pgduckdb/bgw/network.hpp"

#include <unistd.h>

#include "pgduckdb/bgw/utils.hpp"

extern "C" {
#include "postgres.h"
}

namespace pgduckdb {
Network::Network(const int _fd) : fd(_fd) {};

PGDuckDBMessageType
Network::ReadType() const {
	return ReadValue<PGDuckDBMessageType>();
}

void
Network::ReadBuffer(uint8_t *buffer, size_t read_size) const {
	int read_bytes = 0;
	while (read_bytes < read_size) {
		const auto to_read = read_size - read_bytes;
		const int r = read(fd, buffer + read_bytes, to_read);
		if (r < 0) {
			elog(ERROR, "[pgduckdb_bgw] Network::ReadBuffer - failed to read %zu bytes - (%d) %s", to_read, GetErrno(),
			     GetStrError());
		}

		read_bytes += r;
		if (read_bytes < read_size) {
			usleep(100);
		}
	}
}

void
Network::Write(PGDuckDBMessageType t, uint64_t id) const {
	WriteValue(t);
	WriteValue(id);
}

void
Network::Write(PGDuckDBMessageType t) const {
	WriteValue(t);
}

uint64_t
Network::ReadId() const {
	return ReadValue<uint64_t>();
}

void
Network::WriteBuffer(uint8_t *buffer, size_t length) const {
	int written = 0;
	while (written < length) {
		const auto to_write = length - written;
		const int w = write(fd, buffer + written, to_write);
		if (w < 0) {
			elog(ERROR, "[pgduckdb_bgw] Network::WriteBuffer - failed to write %zu bytes - (%d) %s", to_write,
			     GetErrno(), GetStrError());
		}

		written += w;
		if (written < length) {
			usleep(100);
		}
	}
}

void
Network::Close() const {
	close(fd);
}

} // namespace pgduckdb