#pragma once

#include <string>
#include <vector>

namespace quack {

/* constants for quack.secrets */
#define Natts_quack_secret              6
#define Anum_quack_secret_type          1
#define Anum_quack_secret_id            2
#define Anum_quack_secret_secret        3
#define Anum_quack_secret_region        4
#define Anum_quack_secret_endpoint      5
#define Anum_quack_secret_r2_account_id 6

typedef struct QuackSecret {
	std::string type;
	std::string id;
	std::string secret;
	std::string region;
	std::string endpoint;
	std::string r2_account_id;
} QuackSecret;

extern std::vector<QuackSecret> read_quack_secrets();

/* constants for quack.extensions */
#define Natts_quack_extension       2
#define Anum_quack_extension_name   1
#define Anum_quack_extension_enable 2

typedef struct QuackExension {
	std::string name;
	bool enabled;
} QuackExension;

extern std::vector<QuackExension> read_quack_extensions();

} // namespace quack