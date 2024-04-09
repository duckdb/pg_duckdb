#pragma once

#include "postgres.h"

#include "executor/executor.h"

bool quack_execute_select(QueryDesc *query_desc, ScanDirection direction, uint64_t count);