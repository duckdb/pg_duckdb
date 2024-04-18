#pragma once

#include "duckdb/common/allocator.hpp"

namespace quack {

struct QuackAllocatorData : public duckdb::PrivateAllocatorData {
	explicit QuackAllocatorData() {
	}
};

duckdb::data_ptr_t QuackAllocate(duckdb::PrivateAllocatorData *private_data, duckdb::idx_t size);
void QuackFree(duckdb::PrivateAllocatorData *private_data, duckdb::data_ptr_t ptr, duckdb::idx_t idx);
duckdb::data_ptr_t QuackReallocate(duckdb::PrivateAllocatorData *private_data, duckdb::data_ptr_t pointer,
                                   duckdb::idx_t old_size, duckdb::idx_t size);

} // namespace quack