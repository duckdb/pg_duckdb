extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
}

#include "pgduckdb/pgduckdb.h"

extern "C" {
PGDLLEXPORT void
pgduckdb_background_worker_main(Datum main_arg) {
	elog(LOG, "started pgduckdb background worker");
	// Set up a signal handler for SIGTERM
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(duckdb_background_worker_db, NULL, 0);

	while (true) {
		// Initialize SPI (Server Programming Interface) and connect to the database
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		// Execute the query
		auto ret = SPI_execute("select duckdb.query('select * from sync_catalogs_with_pg()');", false, 0);
		if (ret != SPI_OK_SELECT) {
			elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
		}

		// Commit the transaction
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_stat(false);
		pgstat_report_activity(STATE_IDLE, NULL);

		// Wait for a second or until the latch is set
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1000L, PG_WAIT_EXTENSION);
		CHECK_FOR_INTERRUPTS();
		ResetLatch(MyLatch);
	}

	proc_exit(0);
}
}

void
DuckdbInitBackgroundWorker(void) {
	BackgroundWorker worker;

	// Set up the worker struct
	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_duckdb");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "pgduckdb_background_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_duckdb sync worker");
	worker.bgw_restart_time = 1;
	worker.bgw_main_arg = (Datum)0;

	// Register the worker
	RegisterBackgroundWorker(&worker);
}
