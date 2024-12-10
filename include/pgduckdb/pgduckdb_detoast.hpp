#pragma once

#include "pgduckdb/pg/declarations.hpp"
#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

Datum DetoastPostgresDatum(struct varlena *value, bool *should_free);

} // namespace pgduckdb
