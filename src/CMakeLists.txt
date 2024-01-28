ADD_LIBRARY(extension OBJECT quack.c
                             quack_internal.cpp)

set(PROJECT_OBJECTS ${PROJECT_OBJECTS} "$<TARGET_OBJECTS:extension>" PARENT_SCOPE)
