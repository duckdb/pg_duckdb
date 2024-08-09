#pragma once

#include <vector>
#include <string>
#include <sstream>

#include <cstdio>

namespace pgduckdb {

inline std::vector<std::string>
TokenizeString(char *str, const char delimiter) {
	std::vector<std::string> v;
	std::stringstream ss(str); // Turn the string into a stream.
	std::string tok;
	while (getline(ss, tok, delimiter)) {
		v.push_back(tok);
	}
	return v;
};

std::string CreateOrGetDirectoryPath(std::string directoryName);

} // namespace pgduckdb

void DuckdbCreateCacheDirectory(void);
