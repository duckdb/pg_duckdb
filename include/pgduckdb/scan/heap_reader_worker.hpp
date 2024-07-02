#pragma once

extern "C" {
#include "postgres.h"
#include "postmaster/bgworker.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include "storage/s_lock.h"
}

extern "C" {
PGDLLEXPORT void PGDuckDBHeapReaderWorker(Datum arg);
}

namespace pgduckdb {

constexpr int32_t WORKER_SCAN_BLOCK = 32;

typedef struct PostgresScanWorkerSharedState {
	/* Relation */
	BlockNumber n_blocks;
	Oid database;
	Oid relid;
	/* Workers sync */
	uint32 next_scan_start_block;
	slock_t worker_lock;
} PostgresScanWorkerSharedState;

typedef struct PostgresScanThreadWorker {
	dsm_handle workers_global_shared_state;
	Buffer buffer;
	pg_atomic_flag buffer_ready;
	pg_atomic_flag worker_done;
	pg_atomic_flag thread_running;
} PostgresScanThreadWorker;

extern BackgroundWorkerHandle *PGDuckDBStartHeapReaderWorker(Datum arg);

} // namespace pgduckdb
