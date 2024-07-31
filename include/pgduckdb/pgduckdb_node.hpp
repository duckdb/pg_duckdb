#pragma once

extern "C" {
#include "postgres.h"
#include "nodes/extensible.h"
}

extern CustomScanMethods duckdb_scan_scan_methods;
extern "C" void duckdb_init_node(void);