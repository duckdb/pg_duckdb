cmake_minimum_required(VERSION 2.8.12...3.29)

project(CachedHTTPFsExtension)

add_extension_definitions()

include_directories(include ../duckdb/third_party/httplib ../duckdb/parquet/include)

build_static_extension(
  cached_httpfs
  hffs.cpp
  s3fs.cpp
  httpfs.cpp
  http_file_cache.cpp
  http_state.cpp
  crypto.cpp
  create_secret_functions.cpp
  cached_httpfs_extension.cpp)

set(PARAMETERS "-warnings")
build_loadable_extension(
  cached_httpfs
  ${PARAMETERS}
  hffs.cpp
  s3fs.cpp
  httpfs.cpp
  http_file_cache.cpp
  http_state.cpp
  crypto.cpp
  create_secret_functions.cpp
  cached_httpfs_extension.cpp)

if(MINGW)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
endif()

find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
target_link_libraries(cached_httpfs_loadable_extension duckdb_mbedtls
                      ${OPENSSL_LIBRARIES})
target_link_libraries(cached_httpfs_extension duckdb_mbedtls ${OPENSSL_LIBRARIES})

if(MINGW)
  find_package(ZLIB)
  target_link_libraries(cached_httpfs_loadable_extension ZLIB::ZLIB -lcrypt32)
  target_link_libraries(cached_httpfs_extension ZLIB::ZLIB -lcrypt32)
endif()

install(
  TARGETS cached_httpfs_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
