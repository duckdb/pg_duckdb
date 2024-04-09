ADD_LIBRARY(extension OBJECT  quack.c
                              quack_hooks.c
                              quack_select.cpp
                              quack_heap_scan.cpp
                              quack_types.cpp)

set(PROJECT_OBJECTS ${PROJECT_OBJECTS} "$<TARGET_OBJECTS:extension>" PARENT_SCOPE)
