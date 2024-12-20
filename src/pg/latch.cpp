
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"

#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/wait_event.h"
}

namespace pgduckdb::pg {

/*
 * WaitGlobalLatch is used to wait on process postgres its MyLatch. To make
 * sure one thread doesn't miss any updates on the latch, which were received by
 * another thread. We use a global counter to keep track of the number of times
 * the latch was updated. If the counter is different from the last known
 * counter, we know another thread consumed an update that might have been
 * intended for us, so we simply return without waiting.
 *
 * NOTE: We need to take the GlobalProcessLock because WaitLatch can throw
 * errors, which modifies global variables. So to avoid dataraces conditions,
 * we need take the global lock. This also obviously modifies the MyLatch
 * global, so even if WaitLatch did not throw errors. We'd still need to take
 * the global lock for that, or at the very least we'd need to take a specific
 * lock for MyLatch and ensure that no other code modifies MyLatch.
 */
void
WaitMyLatch(uint64_t &last_known_latch_update_count) {
	// A 64bit uint takes 584 years to overflow when it's updated every
	// nanosecond. That's more than enough for any practical purposes. If
	// someone keeps a Postgres connection open for more than 584 years,
	// they deserve what they get.
	static uint64_t latch_update_count = 0;

	std::lock_guard<std::mutex> lock(GlobalProcessLock::GetLock());
	if (last_known_latch_update_count == latch_update_count) {
		PostgresFunctionGuard(WaitLatch, MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, PG_WAIT_EXTENSION);
		/* Note: No need to use PostgresFunctionGuard here, because ResetLatch
		 * is a trivial function */
		ResetLatch(MyLatch);
		latch_update_count++;
	}
	last_known_latch_update_count = latch_update_count;
}

} // namespace pgduckdb::pg
