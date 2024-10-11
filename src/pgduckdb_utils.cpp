#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "storage/fd.h"
}

#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

namespace pgduckdb {

static bool
CheckDirectory(const char *directory) {
	struct stat info;

	if (lstat(directory, &info) != 0) {
		if (errno == ENOENT) {
			elog(DEBUG2, "Directory `%s` doesn't exists.", directory);
			return false;
		} else if (errno == EACCES) {
			elog(ERROR, "Can't access `%s` directory.", directory);
		} else {
			elog(ERROR, "Other error when reading `%s`.", directory);
		}
	}

	if (!S_ISDIR(info.st_mode)) {
		elog(WARNING, "`%s` is not directory.", directory);
	}

	if (access(directory, R_OK | W_OK)) {
		elog(ERROR, "Directory `%s` permission problem.", directory);
	}

	return true;
}

std::string
CreateOrGetDirectoryPath(std::string directory_name) {
	StringInfo duckdb_data_directory = makeStringInfo();
	appendStringInfo(duckdb_data_directory, "%s/%s", DataDir, directory_name.c_str());

	if (!CheckDirectory(duckdb_data_directory->data)) {
		if (MakePGDirectory(duckdb_data_directory->data) == -1) {
			int error = errno;
			elog(ERROR, "Creating %s directory failed with reason `%s`\n", duckdb_data_directory->data,
			     strerror(error));
			pfree(duckdb_data_directory->data);
		}
		elog(DEBUG2, "Created %s directory", duckdb_data_directory->data);
	};

	std::string directory(duckdb_data_directory->data);
	pfree(duckdb_data_directory->data);
	return directory;
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

