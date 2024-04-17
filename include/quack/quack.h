#pragma once

#include <string>

// quack.c
extern "C" void _PG_init(void);

// quack_hooks.c
extern void quack_init_hooks(void);

// scanner.cpp
void load_scanner(std::string name);
