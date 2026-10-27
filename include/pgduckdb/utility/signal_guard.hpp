
#pragma once

#include <signal.h>

namespace pgduckdb {

class ThreadSignalBlockGuard {
public:
	ThreadSignalBlockGuard(const ThreadSignalBlockGuard &) = delete;
	ThreadSignalBlockGuard(ThreadSignalBlockGuard &&) = delete;
	ThreadSignalBlockGuard &operator=(const ThreadSignalBlockGuard &) = delete;
	ThreadSignalBlockGuard &operator=(ThreadSignalBlockGuard &&) = delete;

	explicit ThreadSignalBlockGuard();
	~ThreadSignalBlockGuard();
	void unblock();

private:
	bool _blocked;
	sigset_t _saved_set;
};

/*
 * Postgres saves and restores the signal mask using sigprocmask() in some places, e.g. dsm_impl_posix_resize(). On
 * macOS sigprocmask() changes the signal mask of all threads of the process, not only of the calling thread. When
 * such code runs on a DuckDB thread, which has all signals blocked (see ThreadSignalBlockGuard), restoring the mask
 * blocks all signals on the backend main thread too. The backend then never handles SIGUSR1, and e.g. waits forever
 * for parallel workers to finish.
 *
 * ThreadSignalUnblockGuard should wrap Postgres calls that can run on DuckDB threads. On macOS it temporarily gives the
 * calling DuckDB thread the regular backend signal mask, so that a process-wide restore done by Postgres restores
 * that mask instead of the fully blocked one. It does nothing on other platforms and on the main thread.
 */
class ThreadSignalUnblockGuard {
public:
	ThreadSignalUnblockGuard(const ThreadSignalUnblockGuard &) = delete;
	ThreadSignalUnblockGuard(ThreadSignalUnblockGuard &&) = delete;
	ThreadSignalUnblockGuard &operator=(const ThreadSignalUnblockGuard &) = delete;
	ThreadSignalUnblockGuard &operator=(ThreadSignalUnblockGuard &&) = delete;

	explicit ThreadSignalUnblockGuard();
	~ThreadSignalUnblockGuard();
	/*
	 * Restores the regular backend signal mask when called on the main thread, in case Postgres code that ran on a
	 * DuckDB thread blocked all signals process-wide. Only has an effect on macOS.
	 */
	static void UnblockMainThread();

private:
	bool _changed;
	sigset_t _saved_set;
};

} // namespace pgduckdb
