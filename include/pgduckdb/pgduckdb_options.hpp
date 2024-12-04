#pragma once

#include <string>
#include <vector>

namespace pgduckdb {

/* constants for duckdb.secrets */
#define Natts_duckdb_secret                  11
#define Anum_duckdb_secret_name              1
#define Anum_duckdb_secret_type              2
#define Anum_duckdb_secret_key_id            3
#define Anum_duckdb_secret_secret            4
#define Anum_duckdb_secret_region            5
#define Anum_duckdb_secret_session_token     6
#define Anum_duckdb_secret_endpoint          7
#define Anum_duckdb_secret_r2_account_id     8
#define Anum_duckdb_secret_use_ssl           9
#define Anum_duckdb_secret_scope             10
#define Anum_duckdb_secret_connection_string 11

enum SecretType { S3, R2, GCS, AZURE };

typedef struct DuckdbSecret {
	std::string name;
	SecretType type;
	std::string key_id;
	std::string secret;
	std::string region;
	std::string session_token;
	std::string endpoint;
	std::string r2_account_id;
	bool use_ssl;
	std::string scope;
	std::string connection_string; // Used for Azure
} DuckdbSecret;

std::string SecretTypeToString(SecretType type);

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
