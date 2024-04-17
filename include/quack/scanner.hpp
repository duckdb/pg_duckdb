#pragma once

extern "C" {
#include "postgres.h"

#include "utils/relcache.h"
}

#include <string>
#include <vector>

class Scanner {
public:
	static Scanner *SetupScanner(Relation relation);
	virtual ~Scanner() = 0;
	virtual std::vector<Datum> GetNextChunk(uint32 column_id) = 0;
	virtual std::vector<Oid> GetTypes() = 0;
	virtual uint32 GetColumnCount() = 0;
	virtual uint64 GetCurrentRow(uint32 column_id) = 0;
	virtual std::string GetTableName() = 0;

	Relation relation;
};

extern "C" void RegisterScanner(char *, void *);

/* Registration function for loaded scanners. */
extern "C" void ScannerInit();
