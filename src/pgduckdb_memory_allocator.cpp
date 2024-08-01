#include "duckdb/common/allocator.hpp"

extern "C" {
#include "postgres.h"
}

#include "pgduckdb/pgduckdb_memory_allocator.hpp"

namespace pgduckdb {

duckdb::data_ptr_t
DuckdbAllocate(duckdb::PrivateAllocatorData *private_data, duckdb::idx_t size) {
	return reinterpret_cast<duckdb::data_ptr_t>(palloc(size));
}

void
DuckdbFree(duckdb::PrivateAllocatorData *private_data, duckdb::data_ptr_t pointer, duckdb::idx_t idx) {
	return pfree(pointer);
}

duckdb::data_ptr_t
DuckdbReallocate(duckdb::PrivateAllocatorData *private_data, duckdb::data_ptr_t pointer, duckdb::idx_t old_size,
                 duckdb::idx_t size) {
	return reinterpret_cast<duckdb::data_ptr_t>(repalloc(pointer, size));
}

} // namespace pgduckdb