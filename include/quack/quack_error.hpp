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
	quack::Logger::GetLock().lock(); 									\
	PG_TRY();											\
	{													\
		ereport(elevel, errmsg_internal(__VA_ARGS__)); 	\
	}													\
	PG_CATCH();										\
	{													\
		quack::Logger::GetLock().unlock();								\
		PG_RE_THROW();									\
	}													\
	quack::Logger::GetLock().unlock();									\
	PG_END_TRY();										\
