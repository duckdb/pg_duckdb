#pragma once

extern "C" {
#include "postgres.h"

#include "executor/executor.h"
}

extern "C" bool quack_execute_select(QueryDesc *query_desc, ScanDirection direction, uint64_t count);