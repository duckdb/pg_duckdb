#pragma once

// quack.c
extern int quack_max_threads_per_query;
extern char *quack_secret;
extern "C" void _PG_init(void);

// quack_hooks.c
extern void quack_init_hooks(void);