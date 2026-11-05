#include "pgduckdb/utility/signal_guard.hpp"

#include <stdexcept>

#include <pthread.h>

extern "C" {
#include "postgres.h"
#include "libpq/pqsignal.h"
}

namespace pgduckdb {

ThreadSignalBlockGuard::ThreadSignalBlockGuard() : _blocked(false), _saved_set() {
	sigset_t new_set;
	if (sigfillset(&new_set) < 0) {
		throw std::runtime_error("sigfillset() failed");
	}

	const int err = pthread_sigmask(SIG_BLOCK, &new_set, &_saved_set);
	if (err != 0) {
		throw std::runtime_error("pthread_sigmask(SIG_BLOCK) failed");
	}

	_blocked = true;
}

ThreadSignalBlockGuard::~ThreadSignalBlockGuard() {
	unblock();
}

void
ThreadSignalBlockGuard::unblock() {
	if (!_blocked) {
		return;
	}

	const int err = pthread_sigmask(SIG_SETMASK, &_saved_set, /*old_set*/ nullptr);
	if (err != 0) {
		throw std::runtime_error("pthread_sigmask(SIG_SETMASK) failed");
	}

	_blocked = false;
}

ThreadSignalUnblockGuard::ThreadSignalUnblockGuard() : _changed(false), _saved_set() {
#ifdef __APPLE__
	if (pthread_main_np()) {
		return;
	}

	// Errors are ignored instead of thrown, because this guard is also used in destructors. pthread_sigmask() can only
	// fail for an invalid "how" argument.
	_changed = pthread_sigmask(SIG_SETMASK, &UnBlockSig, &_saved_set) == 0;
#endif
}

ThreadSignalUnblockGuard::~ThreadSignalUnblockGuard() {
	if (_changed) {
		pthread_sigmask(SIG_SETMASK, &_saved_set, /*old_set*/ nullptr);
	}
}

void
ThreadSignalUnblockGuard::UnblockMainThread() {
#ifdef __APPLE__
	if (pthread_main_np()) {
		pthread_sigmask(SIG_SETMASK, &UnBlockSig, /*old_set*/ nullptr);
	}
#endif
}

} // namespace pgduckdb
