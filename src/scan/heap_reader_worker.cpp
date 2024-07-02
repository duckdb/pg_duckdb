
extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include "storage/lwlocknames.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/relcache.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/rel.h"
}

#include "quack/scan/heap_reader_worker.hpp"

#include <iostream>

extern "C" {
static const long wait_delay = 100L;

static void
quackWaitReleaseBuffers(quack::QuackWorkerSharedState *workerSharedState, pg_atomic_flag *workerReleaseFlag,
                        Buffer *buffers) {

	while (pg_atomic_unlocked_test_flag(workerReleaseFlag));

	/* Release buffers that are consumed */
	for (int k = 0; k < workerSharedState->worker_buffer_write_size; k++) {
		if (BufferIsValid(buffers[k])) {
			UnlockReleaseBuffer(buffers[k]);
		}
	}

	pg_atomic_clear_flag(workerReleaseFlag);
}

void
quack_heap_reader_worker(Datum arg) {

	pqsignal(SIGTERM, die);
	uint32 worker_id = 0;
	dsm_segment *segment = nullptr;
	pg_atomic_flag *sharedWorkerReleaseFlagsPtr = nullptr;
	uint8 *sharedBufferStatusPtr = nullptr;
	BlockNumber *sharedBufferBlockNumberPtr = nullptr;
	Buffer *sharedBufferPtr = nullptr;

	BackgroundWorkerUnblockSignals();
	quack::QuackWorkerSharedState *workerSharedState = NULL;

	if (IsBackgroundWorker) {
		segment = dsm_attach(arg);
	} else {
		segment = dsm_find_mapping(arg);
	}

	workerSharedState = (quack::QuackWorkerSharedState *)dsm_segment_address(segment);

	if (IsBackgroundWorker) {
		sharedWorkerReleaseFlagsPtr =
		    (pg_atomic_flag *)dsm_segment_address(segment) + sizeof(quack::QuackWorkerSharedState);
		sharedBufferStatusPtr =
		    (uint8 *)(sharedWorkerReleaseFlagsPtr + sizeof(pg_atomic_flag) * workerSharedState->planned_workers);
		sharedBufferBlockNumberPtr =
		    (BlockNumber *)(sharedBufferStatusPtr + sizeof(uint8) * workerSharedState->expected_buffer_size);
		sharedBufferPtr =
		    (Buffer *)(sharedBufferBlockNumberPtr + sizeof(BlockNumber) * workerSharedState->expected_buffer_size);
	} else {
		sharedWorkerReleaseFlagsPtr = workerSharedState->worker_release_flags;
		sharedBufferStatusPtr = workerSharedState->buffer_status;
		sharedBufferBlockNumberPtr = workerSharedState->buffer_block_number;
		sharedBufferPtr = workerSharedState->buffer;
	}

	worker_id = pg_atomic_fetch_add_u32(&workerSharedState->next_worker_id, 1);
	uint32 workerBufferPosition = worker_id * workerSharedState->worker_buffer_write_size;

	pg_atomic_fetch_add_u32(&workerSharedState->n_active_workers, 1);
	pg_atomic_add_fetch_u32(&workerSharedState->real_buffer_size, workerSharedState->worker_buffer_write_size);

	Buffer *buffers = (Buffer *)palloc0(sizeof(Buffer) * workerSharedState->worker_buffer_write_size);
	Buffer *previousBuffers = (Buffer *)palloc0(sizeof(Buffer) * workerSharedState->worker_buffer_write_size);
	BlockNumber *bufferBlockNumber =
	    (BlockNumber *)palloc0(sizeof(BlockNumber) * workerSharedState->worker_buffer_write_size);
	uint8 *bufferStatus = (uint8 *)palloc0(sizeof(uint8) * workerSharedState->worker_buffer_write_size);

	if (IsBackgroundWorker) {
		BackgroundWorkerInitializeConnectionByOid(workerSharedState->database, InvalidOid, 0);
		StartTransactionCommand();
	}

	Relation relation = RelationIdGetRelation(workerSharedState->relid);
	bool first_run = true;

	for (;;) {

		SpinLockAcquire(&workerSharedState->lock);

		/* Nothing to scan next so finish this worker */
		if (workerSharedState->next_scan_block == workerSharedState->n_blocks) {
			SpinLockRelease(&workerSharedState->lock);
			break;
		}

		uint32 workerStartBlockNumber = workerSharedState->next_scan_block;
		uint32 workerLastBlockNumber = workerStartBlockNumber + workerSharedState->worker_scan_block_size;

		if (workerLastBlockNumber > workerSharedState->n_blocks)
			workerLastBlockNumber = workerSharedState->n_blocks;

		workerSharedState->next_scan_block = workerLastBlockNumber;

		// elog(WARNING, "(%d) RANGE: %d - %d", getpid(), workerStartBlockNumber, workerLastBlockNumber);

		SpinLockRelease(&workerSharedState->lock);

		for (int i = workerStartBlockNumber; i < workerLastBlockNumber;) {

			int bufferReadSize = Min(workerSharedState->worker_buffer_write_size, workerLastBlockNumber - i);

			memset(buffers, InvalidBuffer, sizeof(Buffer) * workerSharedState->worker_buffer_write_size);
			memset(bufferBlockNumber, InvalidBlockNumber,
			       sizeof(BlockNumber) * workerSharedState->worker_buffer_write_size);
			memset(bufferStatus, quack::BUFFER_DONE, sizeof(uint8) * workerSharedState->worker_buffer_write_size);

			/* Read next j buffers and lock them */
			for (int j = 0; j < bufferReadSize; j++) {
				buffers[j] =
				    ReadBufferExtended(relation, MAIN_FORKNUM, i + j, RBM_NORMAL, GetAccessStrategy(BAS_BULKREAD));
				LockBuffer(buffers[j], BUFFER_LOCK_SHARE);
				bufferBlockNumber[j] = i + j;
				bufferStatus[j] = quack::BUFFER_READ;
			}

			if (!first_run) {
				quackWaitReleaseBuffers(workerSharedState, &sharedWorkerReleaseFlagsPtr[worker_id], previousBuffers);
			}

			memset(previousBuffers, InvalidBuffer, sizeof(Buffer) * workerSharedState->worker_buffer_write_size);

			SpinLockAcquire(&workerSharedState->lock);

			memcpy(&sharedBufferPtr[workerBufferPosition], (void *)buffers,
			       sizeof(Buffer) * workerSharedState->worker_buffer_write_size);

			memcpy((void *)previousBuffers, (void *)buffers,
			       sizeof(Buffer) * workerSharedState->worker_buffer_write_size);

			memcpy(&sharedBufferBlockNumberPtr[workerBufferPosition], (void *)bufferBlockNumber,
			       sizeof(BlockNumber) * workerSharedState->worker_buffer_write_size);

			memcpy(&sharedBufferStatusPtr[workerBufferPosition], (void *)bufferStatus,
			       sizeof(uint8) * workerSharedState->worker_buffer_write_size);

			// for (int j = 0; j < bufferReadSize; j++) {
			// 	elog(WARNING, "(%d, %d) quack_heap_reader_worker %d %d %d (%d)", getpid(), worker_id + 1,
			// 	     bufferBlockNumber[j], buffers[j], bufferStatus[j], bufferReadSize);
			// }

			SpinLockRelease(&workerSharedState->lock);

			i += bufferReadSize;

			first_run = false;
		}
	}


	/* Now wait for last batch to be releases */
	quackWaitReleaseBuffers(workerSharedState, &sharedWorkerReleaseFlagsPtr[worker_id], previousBuffers);

	SpinLockAcquire(&workerSharedState->lock);
	memset(bufferStatus, quack::BUFFER_SKIP, sizeof(uint8) * workerSharedState->worker_buffer_write_size);
	memcpy(&sharedBufferStatusPtr[workerBufferPosition], (void *)bufferStatus,
	       sizeof(uint8) * workerSharedState->worker_buffer_write_size);
	SpinLockRelease(&workerSharedState->lock);

	/* IF this is last worker signal that scanning is done */
	if (!pg_atomic_sub_fetch_u32(&workerSharedState->n_active_workers, 1)) {
		pg_atomic_clear_flag(&workerSharedState->scan_next);
	}

	RelationClose(relation);

	if (IsBackgroundWorker) {
		dsm_detach(segment);
		CommitTransactionCommand();
	}

	pfree(buffers);
	pfree(previousBuffers);
	pfree(bufferBlockNumber);
	pfree(bufferStatus);
}

} // extern "C"

namespace quack {

BackgroundWorkerHandle *
quack_start_heap_reader_worker(Datum arg) {
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy(worker.bgw_library_name, "quack");
	strcpy(worker.bgw_function_name, "quack_heap_reader_worker");
	strcpy(worker.bgw_name, "quack reader worker");
	strcpy(worker.bgw_type, "quack reader worker");
	worker.bgw_main_arg = arg;
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("registering dynamic quack reader failed"),
		                errhint("Consider increasing configuration parameter \"max_worker_processes\".")));

	pid_t pid;
	WaitForBackgroundWorkerStartup(handle, &pid);
	return handle;
}

} // namespace quack