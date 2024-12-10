#include "pgduckdb/pg/datum.hpp"

extern "C" {
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif
}

namespace pgduckdb::pg {
int GetDetoastedDatumLen(const Datum value, bool is_bpchar) {
	return is_bpchar ? bpchartruelen(VARDATA_ANY(value), VARSIZE_ANY_EXHDR(value))
	                                   : VARSIZE_ANY_EXHDR(value);
}
const char* GetDetoastedDatumVal(const Datum value) {
	return static_cast<const char *>(VARDATA_ANY(value));
}

bool DatumGetBool(Datum datum) {
	return ::DatumGetBool(datum);
}

char DatumGetChar(Datum datum) {
	return ::DatumGetChar(datum);
}

int16_t DatumGetInt16(Datum datum) {
	return ::DatumGetInt16(datum);
}

int32_t DatumGetInt32(Datum datum) {
	return ::DatumGetInt32(datum);
}

int64_t DatumGetInt64(Datum datum) {
	return ::DatumGetInt64(datum);
}

float DatumGetFloat4(Datum datum) {
	return ::DatumGetFloat4(datum);
}

double DatumGetFloat8(Datum datum) {
	return ::DatumGetFloat8(datum);
}

int32_t DatumGetDateADT(Datum datum) {
	return ::DatumGetDateADT(datum);
}

int64_t DatumGetTimestamp(Datum datum) {
	return ::DatumGetTimestamp(datum);
}

int64_t DatumGetTimestampTz(Datum datum) {
	return ::DatumGetTimestampTz(datum);
}
} // namespace pgduckdb::pg
