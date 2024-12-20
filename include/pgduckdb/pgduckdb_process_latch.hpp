#pragma once

#include <mutex>
#include <condition_variable>
#include <atomic>

extern "C" {
struct Latch;
extern struct Latch *MyLatch;
extern int WaitLatch(Latch *latch, int wakeEvents, long timeout, uint32_t wait_event_info);
extern void ResetLatch(Latch *latch);

/* Defined in storage/latch.h */
#define WL_LATCH_SET        (1 << 0)
#define WL_EXIT_ON_PM_DEATH (1 << 5)

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
	static void
	WaitGlobalLatch() {
		static std::mutex lock;
		static std::condition_variable cv;
		static std::atomic<int> threads_waiting_for_latch_event = 0;
		static bool latch_released = false;
		static std::atomic<bool> latch_event_thread_done = false;

		// We exit if "latch" thread is waiting to finish
		if (!latch_event_thread_done.load()) {
			return;
		}

		if (!threads_waiting_for_latch_event.fetch_add(1)) {
			// Lets start waiting for latch on this thread (we are consider this as "latch" thread)
 			std::unique_lock lk(lock);
			lock.lock();

			WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);

			// Use for notify waiting threads to exit
			latch_released = true;

			// All new threads arriving on latch waiting will return immediately
			latch_event_thread_done = true;

			// Notify one thread waiting
			cv.notify_one();
			lk.unlock();

			// Wait until we are only thread left
			cv.wait(lk, [] { return threads_waiting_for_latch_event = 1; });
			threads_waiting_for_latch_event--;

			// Reset variables
			latch_released = false;
			latch_event_thread_done = false;
		} else {
			std::unique_lock lk(lock);

			// Wait for "latch" thread to notify
			cv.wait(lk, [] { return latch_released; });

			// We are done with this threads
			threads_waiting_for_latch_event--;

			lk.unlock();

			// Notify another thread (either waiting thread or "latch" thread)
			cv.notify_one();
		}
	}
};

} // namespace pgduckdb
