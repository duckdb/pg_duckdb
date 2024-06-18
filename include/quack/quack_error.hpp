#pragma once

#include <mutex>

namespace quack {

struct Logger {
public:
	static std::mutex &GetLock() {
		static std::mutex lock;
		return lock;
	}
};

} // namespace quack

extern "C" {
	#include "postgres.h"
};

#define elog_quack(elevel, ...)  						\
	do {												\
	auto &l = quack::Logger::GetLock();					\
	l.lock(); 											\
	PG_TRY();											\
	{													\
		ereport(elevel, errmsg_internal(__VA_ARGS__)); 	\
	}													\
	PG_CATCH();											\
	{													\
		l.unlock();										\
		PG_RE_THROW();									\
	}													\
	PG_END_TRY();										\
	l.unlock();											\
	} while(0)
