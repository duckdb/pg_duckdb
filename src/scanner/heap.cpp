#include "quack/scanner.hpp"

extern "C" {
#include "postgres.h"
}

#include <string>

void
ScannerInit() {
	elog(WARNING, "registering heap");
	RegisterScanner((char *)"heap", nullptr);
}
