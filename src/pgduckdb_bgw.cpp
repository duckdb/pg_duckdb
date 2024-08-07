extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
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

#include "pgduckdb/pgduckdb.h"

#define WAIT_EVENT_PGDUCKDB_BGW_MAIN 1984
#define PG_DUCKDB_MAGIC 0x00E047C0
#define SHM_QUEUE_SIZE 16384

typedef struct
{
    pid_t worker_pid;
    LWLock lock;
    Latch *worker_latch;
    LWLock	 state_version_lock;
    uint32_t state_version;
    dsm_handle connection_seg[MAX_BACKENDS]; // todo - FLEXIBLE_ARRAY_MEMBER init w/MaxBackends
} PGDuckDBSharedState;

typedef struct
{
    shm_mq_handle *command_queue;
    shm_mq_handle *results_queue;
} PGDuckDBInternalState;

typedef struct
{
    BackendId backend_id;
    Latch *latch;
} PGDuckDBConnectionState;

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

static dsm_handle pgduckdb_create_connection_shm_segment() {
    LWLockAcquire(&pgduckdb_state->lock, LW_EXCLUSIVE);
    elog(INFO, "[con] pgduckdb_create_connection_shm_segment -> starting");

    // Compute queue size
    shm_toc_estimator estimator;
    shm_toc_estimate_chunk(&estimator, SHM_QUEUE_SIZE); // command queue
    shm_toc_estimate_chunk(&estimator, SHM_QUEUE_SIZE); // results queue
    shm_toc_estimate_chunk(&estimator, sizeof(PGDuckDBInternalState)); // internal state

    Size segsize = shm_toc_estimate(&estimator);
    elog(INFO, "[con] pgduckdb_create_connection_shm_segment -> will allocate %zu", segsize);

    // Allocate shared memory
    dsm_segment *seg = dsm_create(segsize, 0);
    shm_toc *toc = shm_toc_create(PG_DUCKDB_MAGIC, dsm_segment_address(seg), segsize);

    pgduckdb_internal_state = (PGDuckDBInternalState *) palloc0(sizeof(PGDuckDBInternalState));

    // Create queue to send messages
    {
        shm_mq *mq = shm_mq_create(shm_toc_allocate(toc, SHM_QUEUE_SIZE), SHM_QUEUE_SIZE);
        shm_toc_insert(toc, 0, mq);
        shm_mq_set_sender(mq, MyProc);
        pgduckdb_internal_state->command_queue = shm_mq_attach(mq, seg, NULL);
        elog(INFO, "[con] pgduckdb_create_connection_shm_segment -> created queue=%p, seg=%p", pgduckdb_internal_state->command_queue, seg);
    }

    // Create queue to receive messages back
    {
        shm_mq *mq = shm_mq_create(shm_toc_allocate(toc, SHM_QUEUE_SIZE), SHM_QUEUE_SIZE);
        shm_toc_insert(toc, 1, mq);
        shm_mq_set_receiver(mq, MyProc);
        pgduckdb_internal_state->results_queue = shm_mq_attach(mq, seg, NULL);
        elog(INFO, "[con] pgduckdb_create_connection_shm_segment -> created queue=%p, seg=%p", pgduckdb_internal_state->results_queue, seg);
    }

    PGDuckDBConnectionState* s = (PGDuckDBConnectionState*)shm_toc_allocate(toc, sizeof(PGDuckDBConnectionState));
    s->backend_id = MyBackendId;
    s->latch = MyLatch;
    shm_toc_insert(toc, 2, s);

    dsm_pin_mapping(seg);

    elog(INFO, "[con] pgduckdb_create_connection_shm_segment -> inserted PGDuckDBConnectionState");

    dsm_handle hdl = dsm_segment_handle(seg);

    LWLockRelease(&pgduckdb_state->lock);

    return hdl;
}

static bool pgduckdb_init_shmem(void)
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
        LWLockInitialize(&pgduckdb_state->state_version_lock, LWLockNewTrancheId());
        pgduckdb_state->worker_pid = InvalidPid;
        pgduckdb_state->worker_latch = NULL;
        for (uint32_t i = 0; i < MAX_BACKENDS; ++i) {
            pgduckdb_state->connection_seg[i] = DSM_HANDLE_INVALID;
        }

        pgduckdb_state->state_version = 0;
    }

    LWLockRelease(AddinShmemInitLock);

    LWLockRegisterTranche(pgduckdb_state->lock.tranche, "pgduckdb");

    return found;
}

void
DuckdbInitBgw(void)
{
    elog(INFO, "DuckdbInitBgw -> starting MyBackendId=%d", MyBackendId);

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

Datum
send_message(PG_FUNCTION_ARGS)
{
    char *message = text_to_cstring(PG_GETARG_TEXT_PP(0));

    if (strlen(message) >= 2048)
        ereport(ERROR, (errmsg("Message too long")));

    elog(INFO, "[con] send_message -> received '%s' | shstate=%p | MyProcPid=%d | MyBackendId=%d | MaxBackends=%d"
        , message, pgduckdb_state, MyProcPid, MyBackendId, MaxBackends);
    pgduckdb_init_shmem();

    elog(INFO, "[con] pid -> %d / workerpid = %d / workerlatch = %p",
        MyProcPid, pgduckdb_state->worker_pid, pgduckdb_state->worker_latch);

    MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);

    if (pgduckdb_internal_state == NULL) {
        Assert(pgduckdb_state->connection_seg[MyBackendId] == DSM_HANDLE_INVALID);

        elog(INFO, "[con] send_message -> creating new shm segment");
        pgduckdb_state->connection_seg[MyBackendId] = pgduckdb_create_connection_shm_segment();
        elog(INFO, "[con] send_message -> pgduckdb_create_connection_shm_segment done for MyBackendId=%u seg=%u; ver=%u",
            MyBackendId, pgduckdb_state->connection_seg[MyBackendId], pgduckdb_state->state_version);
        LWLockAcquire(&pgduckdb_state->state_version_lock, LW_EXCLUSIVE);
        ++pgduckdb_state->state_version; // Increment state version to notify bgw
        LWLockRelease(&pgduckdb_state->state_version_lock);
        elog(INFO, "[con] send_message -> incremented version to %u, will notify %p", pgduckdb_state->state_version, pgduckdb_state->worker_latch);
        SetLatch(pgduckdb_state->worker_latch);
        elog(INFO, "[con] send_message -> notified bgw, will now wait for bgw to attach");
        (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, WAIT_EVENT_MQ_INTERNAL);
        elog(INFO, "[con] send_message -> bgw attached");
        ResetLatch(MyLatch); // not strictly necessary, but cleaner.
    }

    shm_mq_result res = shm_mq_send(pgduckdb_internal_state->command_queue, strlen(message) + 1, message, false, true);
    if (res != SHM_MQ_SUCCESS)
        ereport(ERROR, (errmsg("Failed to send message to bgw")));


    MemoryContextSwitchTo(old_context);

    elog(INFO, "[con] send_message -> done");

    {
        Size received;
        void *data;
        shm_mq_result res = shm_mq_receive(pgduckdb_internal_state->results_queue, &received, &data, false);
        if (res == SHM_MQ_SUCCESS) {
            elog(INFO, "[con] Received message: %s", (char *) data);
        } else if (res == SHM_MQ_DETACHED) {
            elog(INFO, "[con] Message queue detached");
        } else {
            elog(INFO, "[con] Message queue error?");
        }
    }

    PG_RETURN_VOID();
}

extern void die(SIGNAL_ARGS);
extern void procsignal_sigusr1_handler(SIGNAL_ARGS);
extern int * __error(void);

static uint32_t bgw_state_version = 0;

static List* bgw_connections = NULL;

typedef struct {
    shm_mq_handle *command_queue;
    shm_mq_handle *results_queue;
    BackendId backend_id;
} PGDuckDBConnectionDescriptor;

bool has_connection(BackendId id) {
    ListCell *lc;
    foreach(lc, bgw_connections) {
        PGDuckDBConnectionDescriptor* s = (PGDuckDBConnectionDescriptor*)lfirst(lc);
        if (s->backend_id == id) {
            return true;
        }
    }

    return false;
}

void do_update_state(void) {
    for (BackendId i = 0; i < MAX_BACKENDS; ++i) {
        if (pgduckdb_state->connection_seg[i] == DSM_HANDLE_INVALID) {
            // TODO - remove connection from bgw_connections if it's there
            continue;
        }

        if (has_connection(i)) {
            continue;
        }

        // Allocate connection descriptors on TopMemoryContext
        MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
        PGDuckDBConnectionDescriptor* desc = (PGDuckDBConnectionDescriptor*)palloc0(sizeof(PGDuckDBConnectionDescriptor));

        dsm_segment *seg = dsm_attach(pgduckdb_state->connection_seg[i]);
        if (seg == NULL) {
            elog(INFO, "[bgw] Failed to attach to shm segment %u", pgduckdb_state->connection_seg[i]);
            continue;
        }

        shm_toc *toc = shm_toc_attach(PG_DUCKDB_MAGIC, dsm_segment_address(seg));
        PGDuckDBConnectionState* s = (PGDuckDBConnectionState*)shm_toc_lookup(toc, 2, false);
        if (s == NULL) {
            elog(INFO, "[bgw] Failed to lookup connection state");
            continue;
        }

        Assert(s->backend_id == i);
        desc->backend_id = i;

        {
            shm_mq* mq = (shm_mq*)shm_toc_lookup(toc, 0, false);
            shm_mq_set_receiver(mq, MyProc);
            desc->command_queue = shm_mq_attach(mq, seg, NULL);
            elog(INFO, "[bgw] update_state -> got command queue mq=%p", mq);
        }

        {
            shm_mq* mq = (shm_mq*)shm_toc_lookup(toc, 1, false);
            shm_mq_set_sender(mq, MyProc);
            desc->results_queue = shm_mq_attach(mq, seg, NULL);
            elog(INFO, "[bgw] update_state -> got results queue mq=%p", mq);
        }

        bgw_connections = lappend(bgw_connections, desc);
        SetLatch(s->latch);
        elog(INFO, "[bgw] Added connection %u", i);
        MemoryContextSwitchTo(old_context);
    }
}

/*
    Update BGW state from shared memory by iterating over all connection_seg and comparing with our internal list
    - if connection_seg is not in our list, add it
    - if connection_seg is in our list but not in shared memory, remove it

    Returns false if state has not changed, true otherwise
*/
bool update_state(void) {
    uint32_t initial_state_version = bgw_state_version;
    do {

        LWLockAcquire(&pgduckdb_state->state_version_lock, LW_SHARED);
        uint32_t shared_state_version = pgduckdb_state->state_version;
        LWLockRelease(&pgduckdb_state->state_version_lock);

        if (bgw_state_version == shared_state_version) {
            elog(INFO, "[bgw] State is up to date: shared_state_version=%u; initial_state_version=%u", shared_state_version, initial_state_version);
            // We've now converged with the shared state
            return initial_state_version != shared_state_version;
        }

        elog(INFO, "[bgw] Updating state to %u", shared_state_version);
        do_update_state();
        bgw_state_version = shared_state_version;
        elog(INFO, "[bgw] State is now at %u", shared_state_version);
    } while (true);
}

PGDLLEXPORT void pgduckdb_bgw_main(PG_FUNCTION_ARGS)
{
    elog(INFO, "[bgw] Started PGDuckDB BGW");

    pgduckdb_init_shmem();

    pgduckdb_state->worker_latch = MyLatch;

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

        elog(INFO, "[bgw] Ready to do some work!");

        shm_mq_result res;
        Size received;
        void *data;

        ResetLatch(MyLatch);
        (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, WAIT_EVENT_MQ_INTERNAL);

        elog(INFO, "[bgw] Latch set!");

        update_state();

        ListCell *lc;
        foreach(lc, bgw_connections) {
            PGDuckDBConnectionDescriptor* s = (PGDuckDBConnectionDescriptor*)lfirst(lc);
            elog(INFO, "[bgw] Receiving from connection %u", s->backend_id);

            res = shm_mq_receive(s->command_queue, &received, &data, true);
            if (res == SHM_MQ_SUCCESS) {
                char *message = (char *) data;
                elog(INFO, "[bgw] Received message: %s", message);

                shm_mq_result res = shm_mq_send(s->results_queue, strlen(message) + 1, message, false, true);
                if (res != SHM_MQ_SUCCESS)
                    ereport(ERROR, (errmsg("Failed to send message back to cli")));

            } else if (res == SHM_MQ_DETACHED) {
                elog(INFO, "[bgw] Message queue detached");
                break;
            } else if (res == SHM_MQ_WOULD_BLOCK) {
                elog(INFO, "[bgw] Message queue would block");
            } else {
                elog(INFO, "[bgw] Message queue error?");
            }
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
