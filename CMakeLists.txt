set(CMAKE_WARN_DEPRECATED OFF CACHE BOOL "" FORCE)
cmake_minimum_required(VERSION 3.2 FATAL_ERROR)

if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    message(FATAL_ERROR "In-source builds not allowed.
    Please make a new directory and run CMake from there.
    You may need to remove CMakeCache.txt." )
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Build Type - set a default build type `Release` if none was specified
set(PROJECT_DEFAULT_BUILD_TYPE "Release")

if (CMAKE_BUILD_TYPE AND
        NOT CMAKE_BUILD_TYPE MATCHES "^(Debug|Release|RelWithDebInfo|MinSizeRel)$")
    message(FATAL_ERROR "Invalid value for CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}
    valid values are Debug|Release|RelWithDebInfo|MinSizeRel")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to '${PROJECT_DEFAULT_BUILD_TYPE}' as none was specified.")
    set(CMAKE_BUILD_TYPE "${PROJECT_DEFAULT_BUILD_TYPE}" CACHE
        STRING "Choose the type of build." FORCE)
endif()

message(STATUS "CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE}")

project(quack VERSION 0.0.1 LANGUAGES C CXX)

string(TOLOWER "${PROJECT_NAME}" PROJECT_NAME_LOWER)

set(PROJECT_LIB_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}")
set(PROJECT_LIB_NAME "${PROJECT_NAME_LOWER}")

string(TIMESTAMP COMPILATION_DATE "%Y/%m/%d" UTC)

set(POSTGRESQL_MINIMUM_VERSION "16.0.0")
set(BOOST_MINIMUM_VERSION "1.81.0")

message(STATUS "POSTGRESQL_MINIMUM_VERSION=${POSTGRESQL_MINIMUM_VERSION}")
message(STATUS "BOOST_MINIMUM_VERSION=${BOOST_MINIMUM_VERSION}")

# PostgreSQL

find_package(PostgreSQL)

if(NOT PostgreSQL_FOUND OR NOT PostgreSQL_VERSION_STRING)
    message(FATAL_ERROR "PostgreSQL not found - Please check your PostgreSQL installation.")
endif()

# for XbetaY XalphaY XrcY -> X.Y
string(REGEX REPLACE "([0-9]+)[beta|alpha|rc|devel].*" "\\1.0" POSTGRESQL_VERSION_STRING ${PostgreSQL_VERSION_STRING})
STRING(REGEX MATCH "([0-9]+)\.([0-9]+)" POSTGRESQL_VERSION "${PostgreSQL_VERSION_STRING}")

#for X.Y.Z -> XY  Y<10
string(REGEX REPLACE "^([0-9]+)\\.([0-9]+).*" "\\1\\2" PGSQL_VERSION ${POSTGRESQL_VERSION})

if("${POSTGRESQL_VERSION}" VERSION_LESS "${POSTGRESQL_MINIMUM_VERSION}")
    message(FATAL_ERROR " PostgreSQL ${POSTGRESQL_MINIMUM_VERSION} or greater is required.")
endif("${POSTGRESQL_VERSION}" VERSION_LESS "${POSTGRESQL_MINIMUM_VERSION}")

include_directories(${PostgreSQL_SERVER_INCLUDE_DIRS})

# For Apple and Postgres 16 use .dylib instead of .so
if (APPLE AND POSTGRESQL_VERSION VERSION_GREATER_EQUAL "16")
  set(CMAKE_SHARED_MODULE_SUFFIX ".dylib")
endif()

# Boost

find_package(Boost ${BOOST_MINIMUM_VERSION} REQUIRED)

if (NOT Boost_VERSION_MACRO)
    set(Boost_VERSION_MACRO ${Boost_VERSION})
endif()

set(BOOST_VERSION "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")

include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

add_definitions(-DBOOST_ALLOW_DEPRECATED_HEADERS)

# Third party libs

include(third_party/third_party.cmake)

# Compiler

include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)
set(CMAKE_CXX_STANDARD 17)

set(COMPILER_VERSION "${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}")

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# compiler directives
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# https://www.postgresql.org/docs/10/xfunc-c.html


CHECK_C_COMPILER_FLAG("-fPIC" C_COMPILER_SUPPORTS_FPIC)
CHECK_CXX_COMPILER_FLAG("-fPIC" CXX_COMPILER_SUPPORTS_FPIC)

if(C_COMPILER_SUPPORTS_FPIC)
    set(CMAKE_C_FLAGS "-fPIC")
endif()
if(CXX_COMPILER_SUPPORTS_FPIC)
    set(CMAKE_CXX_FLAGS "-fPIC")
endif()

message(STATUS "COMPILER: ${CMAKE_CXX_COMPILER_ID}")

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    # Append CFLAGS/CXXFLAGS and PostgreSQL CFLAGS/CXXFAGS
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${PostgreSQL_CFLAGS} $ENV{CFLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-register ${PostgreSQL_CXXFLAGS} $ENV{CXXFLAGS}")

    # Debug compiler flags
    if(CMAKE_BUILD_TYPE MATCHES "Debug")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
    endif()
    # Release compiler flags
    if(CMAKE_BUILD_TYPE MATCHES "Release")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
    endif()
endif()

message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

# Include directory

include_directories(${PROJECT_SOURCE_DIR}/include)

# Source directories

add_subdirectory("src")

# Library

set(LIBRARY_OUTPUT_PATH lib)

add_library(${PROJECT_LIB_NAME} MODULE ${PROJECT_OBJECTS})

set_target_properties(${PROJECT_LIB_NAME} PROPERTIES
    INSTALL_RPATH "${PostgreSQL_LIBRARY_DIRS}"
)

message(STATUS "PROJECT_LIB_NAME ${PROJECT_LIB_NAME}")

set(LINK_FLAGS "${PostgreSQL_SHARED_LINK_OPTIONS}")

foreach(_dir ${PostgreSQL_SERVER_LIBRARY_DIRS})
  set(LINK_FLAGS "${LINK_FLAGS} -L${_dir}")
endforeach()

if(APPLE)
  set(LINK_FLAGS "${LINK_FLAGS} -bundle_loader ${PG_BINARY} -undefined dynamic_lookup")
endif()

foreach (_third_party_lib ${QUACK_THIRD_PARTY_LIBS})
    target_link_libraries(${PROJECT_LIB_NAME} PUBLIC ${_third_party_lib})
endforeach ()

set_target_properties(${PROJECT_LIB_NAME}
                      PROPERTIES PREFIX ""
                      LINK_FLAGS "${LINK_FLAGS} $ENV{LDFLAGS}")

# PostgreSQL extension control and installation sql files

add_subdirectory(sql)

# Installation

install(TARGETS ${PROJECT_LIB_NAME} DESTINATION ${PostgreSQL_PACKAGE_LIBRARY_DIR})
install(FILES ${PROJECT_FILES_TO_INSTALL} DESTINATION "${PostgreSQL_EXTENSION_DIR}")

# Install third party libraries

foreach (_third_party_lib ${QUACK_THIRD_PARTY_LIBS})
    install(TARGETS ${_third_party_lib} DESTINATION ${PostgreSQL_PACKAGE_LIBRARY_DIR})
endforeach ()

# Add regress test

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    include(CTest)
endif()

add_subdirectory("regress")

# Copy compile-commands.json to ROOT

if (CMAKE_EXPORT_COMPILE_COMMANDS)
    add_custom_target(copy-compile-commands ALL
                      ${CMAKE_COMMAND} -E copy_if_different
                      ${CMAKE_BINARY_DIR}/compile_commands.json
                      ${PROJECT_SOURCE_DIR})
endif()
