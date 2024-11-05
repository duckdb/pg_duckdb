#pragma once

#include <string>
#include <vector>

namespace pgduckdb {

/* constants for duckdb.secrets */
#define Natts_duckdb_secret              9
#define Anum_duckdb_secret_name          1
#define Anum_duckdb_secret_type          2
#define Anum_duckdb_secret_key_id        3
#define Anum_duckdb_secret_secret        4
#define Anum_duckdb_secret_region        5
#define Anum_duckdb_secret_session_token 6
#define Anum_duckdb_secret_endpoint      7
#define Anum_duckdb_secret_r2_account_id 8
#define Anum_duckdb_secret_use_ssl       9

typedef struct DuckdbSecret {
	std::string name;
	std::string type;
	std::string key_id;
	std::string secret;
	std::string region;
	std::string session_token;
	std::string endpoint;
	std::string r2_account_id;
	bool use_ssl;
} DuckdbSecret;

extern std::vector<DuckdbSecret> ReadDuckdbSecrets();

/* constants for duckdb.extensions */
#define Natts_duckdb_extension       2
#define Anum_duckdb_extension_name   1
#define Anum_duckdb_extension_enable 2

typedef struct DuckdbExension {
	std::string name;
	bool enabled;
} DuckdbExtension;

extern std::vector<DuckdbExtension> ReadDuckdbExtensions();

} // namespace pgduckdb
