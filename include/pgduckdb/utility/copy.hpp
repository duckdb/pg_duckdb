#pragma once

extern "C" {
#include "postgres.h"
#include "nodes/plannodes.h"
}

bool DuckdbCopy(PlannedStmt *pstmt, const char *query_string, struct QueryEnvironment *query_env, uint64 *processed,
                bool *is_copy_to_cloud);
