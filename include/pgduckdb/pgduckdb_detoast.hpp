#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
}

#include <mutex>

namespace pgduckdb {

Datum DetoastPostgresDatum(struct varlena *value, bool *should_free);

} // namespace pgduckdb
