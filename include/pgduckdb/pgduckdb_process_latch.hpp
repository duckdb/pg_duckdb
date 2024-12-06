#pragma once

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/wait_event.h"
}

#include <mutex>
#include <condition_variable>

namespace pgduckdb {

/*
 * GlobalProcessLatch is used to wait on process postgres latch. First thread that enters will wait for latch while
 * others will wait until this latch is released.
 */

struct GlobalProcessLatch {
public:
	static void WaitLatch() {
		static std::condition_variable cv;
		static std::mutex lock;
		if (lock.try_lock()) {
			::WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, PG_WAIT_EXTENSION);
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
