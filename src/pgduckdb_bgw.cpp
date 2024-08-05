extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "storage/lwlocknames.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "libpq/pqmq.h"
}

/*
  TODOS:
  - create an array of queues per connection
    -> how to identify a connection?
    -> can we have a hook on new connection and only create queue then?
    -> close / clean up queues when connection is closed
    -> recycle/reuse queues?

*/

#include "pgduckdb/pgduckdb.h"

#define WAIT_EVENT_PGDUCKDB_BGW_MAIN 1984
#define PG_DUCKDB_MAGIC 0x00E047C0
#define SHM_QUEUE_SIZE 16384

typedef struct
{
    pid_t worker_pid;
    LWLock lock;
    Latch *worker_latch;
    dsm_handle seg;
} PGDuckDBSharedState;

typedef struct
{
    shm_mq_handle *input_queue;
    shm_mq_handle *output_queue;
} PGDuckDBInternalState;

static PGDuckDBSharedState *pgduckdb_state = NULL;
static PGDuckDBInternalState *pgduckdb_internal_state = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;

static void
pgduckdb_shmem_request(void)
{
    if (prev_shmem_request_hook) {
      prev_shmem_request_hook();
    }

    elog(INFO, "pgduckdb_shmem_request -> starting");
    RequestAddinShmemSpace(sizeof(PGDuckDBSharedState));
    elog(INFO, "pgduckdb_shmem_request -> done");
}

static void pgduckdb_create_shm_queue() {
    LWLockAcquire(&pgduckdb_state->lock, LW_EXCLUSIVE);
    elog(INFO, "[bgw] pgduckdb_init_shmem -> setting worker_pid=%d", MyProcPid);
    pgduckdb_state->worker_pid = MyProcPid;
    pgduckdb_state->worker_latch = MyLatch;

    // Compute queue size
    shm_toc_estimator estimator;
    shm_toc_estimate_chunk(&estimator, SHM_QUEUE_SIZE);
    shm_toc_estimate_chunk(&estimator, SHM_QUEUE_SIZE);
    Size segsize = shm_toc_estimate(&estimator);

    // Allocate shared memory
    dsm_segment *seg = dsm_create(segsize, 0);
    shm_toc *toc = shm_toc_create(PG_DUCKDB_MAGIC, dsm_segment_address(seg), segsize);

    // Create queue to receive messages
    {
        shm_mq *mq = shm_mq_create(shm_toc_allocate(toc, SHM_QUEUE_SIZE), SHM_QUEUE_SIZE);
        shm_toc_insert(toc, 0, mq);
        shm_mq_set_receiver(mq, MyProc);
        pgduckdb_internal_state->input_queue = shm_mq_attach(mq, seg, NULL);
        elog(INFO, "[bgw] pgduckdb_init_shmem -> created queue=%p, seg=%u", pgduckdb_internal_state->input_queue, pgduckdb_state->seg);
    }

    // Create queue to send messages back
    {
        shm_mq *mq = shm_mq_create(shm_toc_allocate(toc, SHM_QUEUE_SIZE), SHM_QUEUE_SIZE);
        shm_toc_insert(toc, 1, mq);
        shm_mq_set_sender(mq, MyProc);
        pgduckdb_internal_state->output_queue = shm_mq_attach(mq, seg, NULL);
        elog(INFO, "[bgw] pgduckdb_init_shmem -> created queue=%p, seg=%u", pgduckdb_internal_state->output_queue, pgduckdb_state->seg);
    }

    pgduckdb_state->seg = dsm_segment_handle(seg);

    LWLockRelease(&pgduckdb_state->lock);
}

static bool pgduckdb_init_shmem(bool called_from_bgw = false)
{
    bool found;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    pgduckdb_state = (PGDuckDBSharedState*) ShmemInitStruct("pgduckdb",
                                sizeof(PGDuckDBSharedState),
                                &found);
    if (!found)
    {
        /* First time: initializing empty */
        LWLockInitialize(&pgduckdb_state->lock, LWLockNewTrancheId());
        pgduckdb_state->worker_pid = InvalidPid;
        pgduckdb_state->worker_latch = NULL;
        pgduckdb_state->seg = DSM_HANDLE_INVALID;
    }

    if (called_from_bgw) {
        pgduckdb_create_shm_queue();
    }

    LWLockRelease(AddinShmemInitLock);

    LWLockRegisterTranche(pgduckdb_state->lock.tranche, "pgduckdb");

    return found;
}

void
duckdb_init_bgw(void)
{
    elog(INFO, "duckdb_init_bgw -> starting");

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = pgduckdb_shmem_request;

    BackgroundWorker bgw;
    memset(&bgw, 0, sizeof(bgw));
    snprintf(bgw.bgw_name, BGW_MAXLEN, "pgduckdb_main_bgw");
    snprintf(bgw.bgw_type, BGW_MAXLEN, "pgduckdb");
    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS; // TODO: + BGWORKER_BACKEND_DATABASE_CONNECTION

    bgw.bgw_start_time = BgWorkerStart_ConsistentState;
    bgw.bgw_restart_time = 5; /* seconds */
    snprintf(bgw.bgw_library_name, BGW_MAXLEN, "pg_duckdb");
    snprintf(bgw.bgw_function_name, BGW_MAXLEN, "pgduckdb_bgw_main");
    bgw.bgw_main_arg = (Datum)0;
    bgw.bgw_notify_pid = 0;
    RegisterBackgroundWorker(&bgw);
}

extern "C" {

PG_FUNCTION_INFO_V1(send_message);

static shm_mq_handle *commands_queue = NULL;
static shm_mq_handle *results_queue = NULL;

Datum
send_message(PG_FUNCTION_ARGS)
{
    char *message = text_to_cstring(PG_GETARG_TEXT_PP(0));

    if (strlen(message) >= 2048)
        ereport(ERROR, (errmsg("Message too long")));

    elog(INFO, "[cli] send_message -> received '%s' | shstate=%p | MyProcPid=%d", message, pgduckdb_state, MyProcPid);
    pgduckdb_init_shmem();

    elog(INFO, "[cli] pid -> %d / workerpid = %d / workerlatch = %p",
        MyProcPid, pgduckdb_state->worker_pid, pgduckdb_state->worker_latch);

    MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);

    if (commands_queue == NULL) {
        elog(INFO, "[cli] send_message -> attaching to seg=%u", pgduckdb_state->seg);
        dsm_segment *seg = dsm_attach(pgduckdb_state->seg);
        if (seg == NULL)
            ereport(ERROR, (errmsg("Failed to attach to shm segment")));

        shm_toc *toc = shm_toc_attach(PG_DUCKDB_MAGIC, dsm_segment_address(seg));

        {
            shm_mq* mq = (shm_mq*)shm_toc_lookup(toc, 0, false);
            shm_mq_set_sender(mq, MyProc);
            commands_queue = shm_mq_attach(mq, seg, NULL);
            elog(INFO, "[cli] send_message -> got command queue mq=%p", mq);
        }

        {
            shm_mq* mq = (shm_mq*)shm_toc_lookup(toc, 1, false);
            shm_mq_set_receiver(mq, MyProc);
            results_queue = shm_mq_attach(mq, seg, NULL);
            elog(INFO, "[cli] send_message -> got results queue mq=%p", mq);
        }

        dsm_pin_mapping(seg);
    }

    shm_mq_result res = shm_mq_send(commands_queue, strlen(message) + 1, message, false, true);
    if (res != SHM_MQ_SUCCESS)
        ereport(ERROR, (errmsg("Failed to send message to bgw")));


    MemoryContextSwitchTo(old_context);

    elog(INFO, "[cli] send_message -> done");

    {
        Size received;
        void *data;
        shm_mq_result res = shm_mq_receive(results_queue, &received, &data, false);
        if (res == SHM_MQ_SUCCESS) {
            elog(INFO, "[cli] Received message: %s", (char *) data);
        } else if (res == SHM_MQ_DETACHED) {
            elog(INFO, "[cli] Message queue detached");
        } else {
            elog(INFO, "[cli] Message queue error?");
        }
    }

    PG_RETURN_VOID();
}

extern void die(SIGNAL_ARGS);
extern void procsignal_sigusr1_handler(SIGNAL_ARGS);
extern int * __error(void);

PGDLLEXPORT void pgduckdb_bgw_main(PG_FUNCTION_ARGS)
{
    elog(INFO, "[bgw] Started PGDuckDB BGW");

    pgduckdb_internal_state = (PGDuckDBInternalState *) palloc0(sizeof(PGDuckDBInternalState));

    pgduckdb_init_shmem(true);

    // Set sig handlers
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    pqsignal(SIGINT, SignalHandlerForShutdownRequest);
    BackgroundWorkerUnblockSignals();

    elog(INFO, "[bgw] MyLatch=%p; MyProc=%p; MyProc->procLatch=%p; MyProcPid=%d",
        MyLatch, MyProc, &MyProc->procLatch, MyProcPid);

    ResetLatch(MyLatch);

    for (;;)
    {
        MemoryContext subctx;
        MemoryContext oldctx;

        CHECK_FOR_INTERRUPTS();

        /* Use temporary context to avoid leaking memory across cycles. */
        subctx = AllocSetContextCreate(TopMemoryContext,
                        "PGDuckDB BGW sublist",
                        ALLOCSET_DEFAULT_SIZES);
        oldctx = MemoryContextSwitchTo(subctx);

        elog(INFO, "[bgw] Ready to do some work! - here");

        shm_mq_result res;
        Size received;
        void *data;

        ResetLatch(MyLatch);
        (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
                         WAIT_EVENT_MQ_INTERNAL);

        elog(INFO, "[bgw] Latch set! - there");

        res = shm_mq_receive(pgduckdb_internal_state->input_queue, &received, &data, false);
        if (res == SHM_MQ_SUCCESS) {
            char *message = (char *) data;
            elog(INFO, "[bgw] Received message: %s", message);

            shm_mq_result res = shm_mq_send(pgduckdb_internal_state->output_queue, strlen(message) + 1, message, false, true);
            if (res != SHM_MQ_SUCCESS)
                ereport(ERROR, (errmsg("Failed to send message back to cli")));

        } else if (res == SHM_MQ_DETACHED) {
            elog(INFO, "[bgw] Message queue detached");
            break;
        } else {
            elog(INFO, "[bgw] Message queue error?");
        }

        /* Switch back to original memory context. */
        MemoryContextSwitchTo(oldctx);
        /* Clean the temporary memory. */
        MemoryContextDelete(subctx);

        if (ConfigReloadPending)
        {
            elog(INFO, "Config reload pending");
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
    }
}
} // extern "C"
