
#include "pgduckdb/pgduckdb_duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/proc.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "storage/ipc.h"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/bgw/main.hpp"
#include "pgduckdb/bgw/utils.hpp"
#include <thread>
#include <sys/socket.h>
#include <poll.h>
#include <sys/un.h>
#include <unistd.h>

void
DuckdbInitBgw(void) {
	elog(INFO, "DuckdbInitBgw -> starting MyBackendId=%d", MyBackendId);

	BackgroundWorker bgw;
	memset(&bgw, 0, sizeof(bgw));
	snprintf(bgw.bgw_name, BGW_MAXLEN, "pgduckdb_main_bgw");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "pgduckdb");
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

	bgw.bgw_start_time = BgWorkerStart_ConsistentState;
	bgw.bgw_restart_time = 5; /* seconds */
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "pg_duckdb");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "pgduckdb_bgw_main");
	bgw.bgw_main_arg = (Datum)0;
	bgw.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&bgw);
}

extern "C" {

extern void die(SIGNAL_ARGS);
extern void procsignal_sigusr1_handler(SIGNAL_ARGS);

PGDLLEXPORT void
pgduckdb_bgw_main(PG_FUNCTION_ARGS) {
	elog(INFO, "[bgw] Started PGDuckDB BGW");

	// Set sig handlers
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	elog(INFO, "[bgw] MyLatch=%p; MyProc=%p; MyProc->procLatch=%p; MyProcPid=%d", MyLatch, MyProc, &MyProc->procLatch,
	     MyProcPid);

	int sockfd = -1;
	if ((sockfd = socket(AF_LOCAL, SOCK_STREAM, 0)) == -1) {
		pgduckdb::ELogError("pgduckdb_bgw", "socket");
	}

	const auto socket_path = pgduckdb::PGDUCKDB_SOCKET_PATH;
	{
		// Remove the socket file if it exists but don't fail if it doesn't
		auto ret = remove(socket_path);
		if (ret == -1 && errno != ENOENT) {
			elog(WARNING, "[pgduckdb_bgw] could not remove '%s' remove: %s", socket_path, pgduckdb::GetStrError());
		}
	}

	{
		// Bind the socket
		struct sockaddr_un name;
		name.sun_family = AF_LOCAL;
		strncpy(name.sun_path, socket_path, sizeof(name.sun_path));
		name.sun_path[sizeof(name.sun_path) - 1] = '\0';

		if (bind(sockfd, (const struct sockaddr *)&name, SUN_LEN(&name)) == -1) {
			close(sockfd);
			pgduckdb::ELogError("pgduckdb_bgw", "bind");
		}
	}

	elog(INFO, "server: bind: success '%s'", socket_path);

	if (listen(sockfd, 10 /* queue depth */) == -1) {
		pgduckdb::ELogError("pgduckdb_bgw", "listen");
	}

	struct sigaction sa;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		pgduckdb::ELogError("pgduckdb_bgw", "sigaction");
	}

	struct pollfd fds[1];
	fds[0].fd = sockfd;
	fds[0].events = POLLIN;

	auto &db = pgduckdb::DuckDBManager::Get().GetDatabase();

	std::vector<std::thread> threads;
	while (1) {
		if (proc_exit_inprogress || ShutdownRequestPending || ProcDiePending) {
			return;
		}

		int activity = poll(fds, 1, -1);
		elog(DEBUG1, "[pgduck_bgw] poll: got activity=%d on socket", activity);
		if (activity == -1) {
			int errn = errno;
			if (errn != EINTR) {
				elog(WARNING, "[pgduck_bgw] poll: %s", pgduckdb::GetStrError());
			}

			continue;
		} else if (activity == 0 || !(fds[0].revents & POLLIN)) {
			continue;
		}

		int client_fd = accept(sockfd, NULL, NULL);
		if (client_fd == -1) {
			// Something wrong happened with the client, just log it and continue
			elog(WARNING, "[pgduck_bgw] accept: %s", pgduckdb::GetStrError());
			continue;
		}

		// TODO: handle disconnection
		threads.emplace_back(std::thread([client_fd, &db]() { pgduckdb::OnConnection(client_fd, db); }));
	}
}
} // extern "C"