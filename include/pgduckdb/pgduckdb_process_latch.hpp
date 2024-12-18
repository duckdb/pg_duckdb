#pragma once

#include <mutex>
#include <condition_variable>

extern "C" {
struct Latch;
extern struct Latch *MyLatch;
extern int	WaitLatch(Latch *latch, int wakeEvents, long timeout,
					  uint32_t wait_event_info);
extern void ResetLatch(Latch *latch);

/* Defined in storage/latch.h */
#define WL_LATCH_SET (1 << 0)
#define WL_EXIT_ON_PM_DEATH	 (1 << 5)

/* Defined in utils/wait_event.h */
#define PG_WAIT_EXTENSION 0x07000000U
}

namespace pgduckdb {

/*
 * GlobalProcessLatch is used to wait on process postgres latch. First thread that enters will wait for latch while
 * others will wait until this latch is released.
 */

struct GlobalProcessLatch {
public:
	static void WaitGlobalLatch() {
		static std::condition_variable cv;
		static std::mutex lock;
		if (lock.try_lock()) {
			WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
			cv.notify_all();
			lock.unlock();
		} else {
			std::unique_lock<std::mutex> lk(lock);
			cv.wait(lk);
		}
	}
};

} // namespace pgduckdb
