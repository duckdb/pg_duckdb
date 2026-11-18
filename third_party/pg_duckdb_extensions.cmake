duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG f71cfdb4bff85b9e15b1f3b842d9aa395aff0167
)
# Static builds of pg_duckdb link httpfs in, the others load it as an extension
if(PG_DUCKDB_STATIC_HTTPFS)
    duckdb_extension_statically_link(httpfs)
endif()

# Iceberg reads its manifests with avro, which is linked in instead of loaded from the extension repository. The
# avro builds there export all their symbols, also those of the jansson they bundle. On Linux the json_object()
# SQL function of the postgres executable then takes the place of jansson's json_object(), and avro crashes when
# it parses a schema.
duckdb_extension_load(avro
    GIT_URL https://github.com/duckdb/duckdb-avro
    GIT_TAG 4176191c961fe5c0763976b8b869e30ba0e93054
    SUBMODULES "third_party/avro-c"
    APPLY_PATCHES
)
duckdb_extension_statically_link(avro)

# avro-c links jansson, which is built here as a static library with hidden symbols, so that avro binds to it and
# not to the json_* functions of Postgres. avro-c looks for jansson with find_package(jansson CONFIG), which finds
# this target through the empty config file written below, also when vcpkg provides its own jansson.
if(NOT TARGET jansson)
    include(FetchContent)
    FetchContent_Declare(jansson
        URL https://github.com/akheron/jansson/releases/download/v2.14.1/jansson-2.14.1.tar.gz
        URL_HASH SHA256=2521cd51a9641d7a4e457f7215a4cd5bb176f690bc11715ddeec483e85d9e2b3
    )
    set(JANSSON_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    set(JANSSON_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(JANSSON_WITHOUT_TESTS ON CACHE BOOL "" FORCE)
    set(JANSSON_BUILD_DOCS OFF CACHE BOOL "" FORCE)
    set(JANSSON_INSTALL OFF CACHE BOOL "" FORCE)
    # jansson declares cmake_minimum_required(VERSION 3.1), which CMake 4 no longer accepts
    set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
    FetchContent_MakeAvailable(jansson)
    unset(CMAKE_POLICY_VERSION_MINIMUM)
    set_target_properties(jansson PROPERTIES C_VISIBILITY_PRESET hidden)
    # jansson only sets its include directories for its own directory
    target_include_directories(jansson INTERFACE $<BUILD_INTERFACE:${jansson_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${jansson_BINARY_DIR}/include>)
    add_library(jansson::jansson ALIAS jansson)
    # DuckDB exports the extensions it links in, and with avro the libraries it links
    install(TARGETS jansson
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
    file(WRITE ${CMAKE_BINARY_DIR}/pg_duckdb_jansson/janssonConfig.cmake
        "# The jansson target is defined by pg_duckdb_extensions.cmake\n")
    set(jansson_DIR ${CMAKE_BINARY_DIR}/pg_duckdb_jansson CACHE PATH "" FORCE)
endif()

# avro-c passes functions with arguments where it declares function pointers without a prototype, which C23 (the
# default of GCC 15) no longer accepts
set(CMAKE_C_STANDARD 17)

# Static builds of pg_duckdb link the avro extension archive, so they also need the libraries it links: avro-c,
# jansson, the C++ API and the compression libraries, which come from vcpkg or the system. Once all targets exist,
# write them as linker arguments to a file that the pg_duckdb Makefile adds to its link line.
if(PG_DUCKDB_STATIC_HTTPFS)
    # The imported targets of the compression libraries are only visible in the avro-c directory otherwise
    set(CMAKE_FIND_PACKAGE_TARGETS_GLOBAL ON)

    function(pg_duckdb_collect_link_args ITEM VISITED_VAR ARGS_VAR)
        set(visited ${${VISITED_VAR}})
        set(args ${${ARGS_VAR}})
        # skip the directory markers CMake adds to LINK_LIBRARIES, and items that were already added
        if(ITEM MATCHES "^::@" OR ITEM IN_LIST visited)
            return()
        endif()
        list(APPEND visited "${ITEM}")
        if(TARGET ${ITEM})
            get_target_property(type ${ITEM} TYPE)
            if(NOT type STREQUAL "INTERFACE_LIBRARY")
                list(APPEND args "$<TARGET_LINKER_FILE:${ITEM}>")
            endif()
            get_target_property(imported ${ITEM} IMPORTED)
            if(imported OR type STREQUAL "INTERFACE_LIBRARY")
                get_target_property(deps ${ITEM} INTERFACE_LINK_LIBRARIES)
            else()
                get_target_property(deps ${ITEM} LINK_LIBRARIES)
            endif()
            if(deps)
                foreach(dep IN LISTS deps)
                    pg_duckdb_collect_link_args("${dep}" visited args)
                endforeach()
            endif()
        elseif(ITEM MATCHES "::")
            message(FATAL_ERROR "pg_duckdb: avro links the target ${ITEM}, which is not visible here")
        else()
            list(APPEND args "${ITEM}")
        endif()
        set(${VISITED_VAR} ${visited} PARENT_SCOPE)
        set(${ARGS_VAR} ${args} PARENT_SCOPE)
    endfunction()

    function(pg_duckdb_write_static_link_args)
        # The C++ API is part of DuckDB, so leave out what it links, which libduckdb_static.a contains
        set(visited)
        set(args "$<TARGET_LINKER_FILE:duckdb_cpp_api>")
        pg_duckdb_collect_link_args(avro-static visited args)
        list(JOIN args " " args)
        file(GENERATE OUTPUT ${CMAKE_BINARY_DIR}/pg_duckdb_static_link_args.txt CONTENT "${args}\n")
    endfunction()
    cmake_language(DEFER CALL pg_duckdb_write_static_link_args)
endif()
