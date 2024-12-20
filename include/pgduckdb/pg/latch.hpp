#pragma once

#include <cstdint>

namespace pgduckdb::pg {
void WaitMyLatch(uint64_t &last_known_latch_update_count);
} // namespace pgduckdb::pg
