#pragma once

#include "duckdb/common/allocator.hpp"

namespace pgduckdb {

struct DuckdbAllocatorData : public duckdb::PrivateAllocatorData {
	explicit DuckdbAllocatorData() {
	}
};

duckdb::data_ptr_t DuckdbAllocate(duckdb::PrivateAllocatorData *private_data, duckdb::idx_t size);
void DuckdbFree(duckdb::PrivateAllocatorData *private_data, duckdb::data_ptr_t ptr, duckdb::idx_t idx);
duckdb::data_ptr_t DuckdbReallocate(duckdb::PrivateAllocatorData *private_data, duckdb::data_ptr_t pointer,
                                    duckdb::idx_t old_size, duckdb::idx_t size);

} // namespace pgduckdb
