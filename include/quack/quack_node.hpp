#pragma once

extern "C" {
#include "postgres.h"
#include "nodes/extensible.h"
}

extern CustomScanMethods quack_scan_scan_methods;
extern "C" void quack_init_node(void);