#pragma once

extern "C" {
#include "postgres.h"
#include "postmaster/bgworker.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/lwlock.h"
#include "storage/lwlocknames.h"
#include "storage/s_lock.h"
}

extern "C" {
PGDLLEXPORT void quack_heap_reader_worker(Datum arg);
}

namespace quack {

enum BufferStatus { BUFFER_INVALID, BUFFER_READ, BUFFER_ASSIGNED, BUFFER_DONE, BUFFER_SKIP };

typedef struct QuackWorkerSharedState {
	slock_t lock;

	/* Relation */
	BlockNumber n_blocks;
	Oid database;
	Oid relid;
	BlockNumber next_scan_block;
	pg_atomic_flag scan_next;

	/* Worker synchronizaton */
	pg_atomic_uint32 n_active_workers;
	pg_atomic_uint32 next_worker_id;
	uint16 planned_workers;
	pg_atomic_flag *worker_release_flags;

	/* Worker - relation */
	uint16 worker_buffer_write_size;
	uint32 worker_scan_block_size;
	uint16 expected_buffer_size;
	pg_atomic_uint32 real_buffer_size;

	/* Buffer/worker shared memory */
	uint32 next_buffer_idx;
	uint8 *buffer_status;
	BlockNumber *buffer_block_number;
	Buffer *buffer;
} QuackWorkerSharedState;

extern BackgroundWorkerHandle *quack_start_heap_reader_worker(Datum arg);

} // namespace quack