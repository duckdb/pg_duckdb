#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {
bool IsDuckdbTable(Oid relid);
bool IsDuckdbTableAm(const TableAmRoutine *am);
}
