#include "duckdb/common/allocator.hpp"

extern "C" {
#include "postgres.h"
}

#include "quack/quack_memory_allocator.hpp"

namespace quack {

duckdb::data_ptr_t
QuackAllocate(duckdb::PrivateAllocatorData *private_data, duckdb::idx_t size) {
	return reinterpret_cast<duckdb::data_ptr_t>(palloc(size));
}

void
QuackFree(duckdb::PrivateAllocatorData *private_data, duckdb::data_ptr_t pointer, duckdb::idx_t idx) {
	return pfree(pointer);
}

duckdb::data_ptr_t
QuackReallocate(duckdb::PrivateAllocatorData *private_data, duckdb::data_ptr_t pointer, duckdb::idx_t old_size,
                duckdb::idx_t size) {
	return reinterpret_cast<duckdb::data_ptr_t>(repalloc(pointer, size));
}

} // namespace quack