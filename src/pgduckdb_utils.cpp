#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "storage/fd.h"
#include "executor/spi.h"
}

#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

namespace pgduckdb {

static bool
CheckDirectory(const std::string &directory) {
	struct stat info;

	if (lstat(directory.c_str(), &info) != 0) {
		if (errno == ENOENT) {
			elog(DEBUG2, "Directory `%s` doesn't exists.", directory.c_str());
			return false;
		} else if (errno == EACCES) {
			throw std::runtime_error("Can't access `" + directory + "` directory.");
		} else {
			throw std::runtime_error("Other error when reading `" + directory + "`.");
		}
	}

	if (!S_ISDIR(info.st_mode)) {
		elog(WARNING, "`%s` is not directory.", directory.c_str());
	}

	if (access(directory.c_str(), R_OK | W_OK)) {
		throw std::runtime_error("Directory `" + std::string(directory) + "` permission problem.");
	}

	return true;
}

std::string
CreateOrGetDirectoryPath(const char* directory_name) {
	std::ostringstream oss;
	oss << DataDir << "/" << directory_name;
	const auto duckdb_data_directory = oss.str();

	if (!CheckDirectory(duckdb_data_directory)) {
		if (MakePGDirectory(duckdb_data_directory.c_str()) == -1) {
			throw std::runtime_error("Creating data directory '" + duckdb_data_directory + "' failed: `" +
			                         strerror(errno) + "`");
		}

		elog(DEBUG2, "Created %s directory", duckdb_data_directory.c_str());
	};

	return duckdb_data_directory;
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBQueryOrThrow(duckdb::ClientContext &context, const std::string &query) {
	auto res = context.Query(query, false);
	if (res->HasError()) {
		res->ThrowError();
	}
	return res;
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBQueryOrThrow(duckdb::Connection &connection, const std::string &query) {
	return DuckDBQueryOrThrow(*connection.context, query);
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBQueryOrThrow(const std::string &query) {
	auto connection = pgduckdb::DuckDBManager::CreateConnection();
	return DuckDBQueryOrThrow(*connection, query);
}

} // namespace pgduckdb

