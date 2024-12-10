#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb::pg {
int GetDetoastedDatumLen(const Datum value, bool is_bpchar);
const char* GetDetoastedDatumVal(const Datum value);

bool DatumGetBool(Datum datum);
char DatumGetChar(Datum datum);
int16_t DatumGetInt16(Datum datum);
int32_t DatumGetInt32(Datum datum);
int64_t DatumGetInt64(Datum datum);
float DatumGetFloat4(Datum datum);
double DatumGetFloat8(Datum datum);
int32_t DatumGetDateADT(Datum datum);
int64_t DatumGetTimestamp(Datum datum);
int64_t DatumGetTimestampTz(Datum datum);

} // namespace pgduckdb::pg
