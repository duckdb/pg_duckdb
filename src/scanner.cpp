#include <dlfcn.h>

#include <string>
#include <unordered_map>

extern "C" {
#include "postgres.h"

#include "miscadmin.h"
}

#include "quack/scanner.hpp"

std::unordered_map<std::string, Scanner *(*)(Relation)> scanners;

extern "C" void
RegisterScanner(char *name, void *ptr) {
	Scanner *(*scanner)(Relation) = (Scanner * (*)(Relation)) ptr;
	if (scanners[name] != nullptr) {
		elog(WARNING, "Registering scanner %s, which already exists.", name);
	}

	scanners[name] = scanner;
}

void
load_scanner(std::string name) {
	std::string path = std::string(pkglib_path) + "/" + name + ".so";
	void *shared_obj = dlopen(path.c_str(), RTLD_NOW);

	if (shared_obj == nullptr) {
		elog(WARNING, "Unable to read scanner %s: %s", name.c_str(), dlerror());
		elog(WARNING, "Path: %s", path.c_str());

		return;
	}

	void (*init_scanner)(void) = (void (*)(void))dlsym(shared_obj, "ScannerInit");

	if (init_scanner == nullptr) {
		elog(WARNING, "Unable to find ScannerInit in scanner %s", name.c_str());

		return;
	}

	init_scanner();
	elog(DEBUG5, "Scanner %s initialized", name.c_str());
}
