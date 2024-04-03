ADD_LIBRARY(extension OBJECT  quack_internal.cpp
                              quack_hooks.cpp
                              quack_scan.cpp
                              quack_tableam.cpp
                              quack_utility.cpp
                              quack_write_manager.cpp
                              quack.cpp)

set(PROJECT_OBJECTS ${PROJECT_OBJECTS} "$<TARGET_OBJECTS:extension>" PARENT_SCOPE)
