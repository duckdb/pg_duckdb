
extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "access/heapam.h"
#include "postmaster/bgworker.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "tcop/tcopprot.h"
}

#include "pgduckdb/scan/heap_reader_worker.hpp"

extern "C" {

void
PGDuckDBHeapReaderWorker(Datum arg) {
	pqsignal(SIGTERM, die);
	dsm_segment *thread_worker_segment = nullptr, *worker_shared_state_segment = nullptr;
	BufferAccessStrategy strategy = GetAccessStrategy(BAS_BULKREAD);

	BackgroundWorkerUnblockSignals();

	pgduckdb::PostgresScanThreadWorker *thread_worker_shared_state = NULL;
	pgduckdb::PostgresScanWorkerSharedState *worker_shared_state = NULL;

	thread_worker_segment = dsm_attach(arg);
	thread_worker_shared_state = (pgduckdb::PostgresScanThreadWorker *)dsm_segment_address(thread_worker_segment);

	worker_shared_state_segment = dsm_attach(thread_worker_shared_state->workers_global_shared_state);
	worker_shared_state = (pgduckdb::PostgresScanWorkerSharedState *)dsm_segment_address(worker_shared_state_segment);

	BackgroundWorkerInitializeConnectionByOid(worker_shared_state->database, InvalidOid, 0);
	StartTransactionCommand();

	Relation relation = RelationIdGetRelation(worker_shared_state->relid);
	bool thread_running = true;

	while (thread_running) {

		SpinLockAcquire(&worker_shared_state->worker_lock);

		/* Nothing to scan next so finish this worker */
		if (worker_shared_state->next_scan_start_block == worker_shared_state->n_blocks) {
			SpinLockRelease(&worker_shared_state->worker_lock);
			break;
		}

		uint32 worker_start_scan_block = worker_shared_state->next_scan_start_block;
		uint32 worker_end_scan_block = worker_start_scan_block + pgduckdb::WORKER_SCAN_BLOCK;

		if (worker_end_scan_block > worker_shared_state->n_blocks)
			worker_end_scan_block = worker_shared_state->n_blocks;

		worker_shared_state->next_scan_start_block = worker_end_scan_block;

		SpinLockRelease(&worker_shared_state->worker_lock);

		/* Let's fetch */
		for (int i = worker_start_scan_block; i < worker_end_scan_block; i++) {

			if (!thread_running) {
				break;
			}

			Buffer buffer = ReadBufferExtended(relation, MAIN_FORKNUM, i, RBM_NORMAL, strategy);
			LockBuffer(buffer, BUFFER_LOCK_SHARE);

			/* is previous buffer done */
			while (!pg_atomic_unlocked_test_flag(&thread_worker_shared_state->buffer_ready)) {
				if (pg_atomic_unlocked_test_flag(&thread_worker_shared_state->thread_running)) {
					thread_running = false;
					break;
				}
			}

			/* Thread exited so we need do cleanup current buffer */
			if (!thread_running) {
				UnlockReleaseBuffer(buffer);
			}

			Buffer previous_buffer = thread_worker_shared_state->buffer;

			thread_worker_shared_state->buffer = buffer;
			pg_atomic_test_set_flag(&thread_worker_shared_state->buffer_ready);

			if (BufferIsValid(previous_buffer)) {
				UnlockReleaseBuffer(previous_buffer);
			}
		}
	}

	if (thread_running) {
		/* We are out of blocks fo reading so wait for last buffer to be done */
		while (!pg_atomic_unlocked_test_flag(&thread_worker_shared_state->buffer_ready)) {
			if (pg_atomic_unlocked_test_flag(&thread_worker_shared_state->thread_running)) {
				thread_running = false;
				break;
			}
		}
		UnlockReleaseBuffer(thread_worker_shared_state->buffer);
	}

	pg_atomic_test_set_flag(&thread_worker_shared_state->worker_done);

	FreeAccessStrategy(strategy);
	RelationClose(relation);

	dsm_detach(worker_shared_state_segment);
	dsm_detach(thread_worker_segment);

	CommitTransactionCommand();
}

} // extern "C"

namespace pgduckdb {

BackgroundWorkerHandle *
PGDuckDBStartHeapReaderWorker(Datum arg) {
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy(worker.bgw_library_name, "pg_duckdb");
	strcpy(worker.bgw_function_name, "PGDuckDBHeapReaderWorker");
	strcpy(worker.bgw_name, "pgduckdb reader worker");
	strcpy(worker.bgw_type, "pgduckdb reader worker");
	worker.bgw_main_arg = arg;
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		return nullptr;

	pid_t pid;
	WaitForBackgroundWorkerStartup(handle, &pid);
	return handle;
}

} // namespace pgduckdb
