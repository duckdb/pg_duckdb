#pragma once

#include <string.h>

inline bool
AreStringEqual(const char *str1, const char *str2) {
	return str1 == nullptr ? str1 == str2 : strcmp(str1, str2) == 0;
}

inline bool
IsEmptyString(const char *str) {
	return AreStringEqual(str, "");
}
