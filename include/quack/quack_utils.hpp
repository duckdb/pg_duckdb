#pragma once

#include <vector>
#include <string>
#include <sstream>

#include <cstdio>
#include <cstdarg>
#include <cstring>

namespace quack {

inline std::vector<std::string>
tokenizeString(char *str, const char delimiter) {
	std::vector<std::string> v;
	std::stringstream ss(str); // Turn the string into a stream.
	std::string tok;
	while (getline(ss, tok, delimiter)) {
		v.push_back(tok);
	}
	return v;
};

} // namespace quack