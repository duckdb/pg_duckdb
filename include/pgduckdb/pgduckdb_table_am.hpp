extern "C" {
#include "postgres.h"
#include "access/tableam.h"
}

namespace pgduckdb {
bool IsDuckdbTableAm(const TableAmRoutine *am);
}
