#pragma once

extern "C" {
#include "postgres.h"
#include "nodes/plannodes.h"
}

extern bool duckdb_copy(PlannedStmt *pstmt, const char *queryString, struct QueryEnvironment *queryEnv,
                        uint64 *processed);
