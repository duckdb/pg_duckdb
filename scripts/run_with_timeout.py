#!/usr/bin/env python3
"""
Runs a command with a time limit. When the limit is reached, it collects
diagnostics for a hang before it kills the command:

- the state and kernel wait channel of all threads of all postgres processes
- pg_stat_activity and pg_locks of all postgres servers, using psql
- the stack traces of all threads of all postgres processes, using gdb
- the Python tracebacks of all threads of the Python processes started by the
  command, which pytest's faulthandler prints to stderr on SIGABRT
- the log files in the given log directory

Only works on Linux, because it finds the processes in /proc.

Usage: run_with_timeout.py --timeout SECONDS --output DIR [--logs DIR] -- COMMAND...
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

# Attaching gdb to many processes takes a while, so limit how many we attach to
MAX_GDB_PROCESSES = 50
GDB_TIMEOUT = 60


def read_process(pid):
    """Returns (comm, cmdline, session id, uid) of a process, or None if it's gone"""
    proc = Path("/proc") / str(pid)
    try:
        comm = (proc / "comm").read_text().strip()
        cmdline = (
            (proc / "cmdline")
            .read_bytes()
            .replace(b"\0", b" ")
            .decode(errors="replace")
            .strip()
        )
        # The comm field (2) can contain spaces, so split after its closing parenthesis
        stat_fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
        session_id = int(stat_fields[3])
        uid = proc.stat().st_uid
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    return comm, cmdline, session_id, uid


def list_processes():
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            info = read_process(int(entry.name))
            if info:
                yield (int(entry.name), *info)


def can_sudo():
    if os.getuid() == 0 or not shutil.which("sudo"):
        return False
    return subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode == 0


def run_to_file(command, path, header, timeout):
    """Runs a command, appending its output to a file. Returns the output it wrote."""
    with open(path, "a") as f:
        f.write(f"# {header}\n")
        f.flush()
        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=timeout,
            )
            output = result.stdout.decode(errors="replace")
        except subprocess.TimeoutExpired:
            output = f"{' '.join(command)} did not finish in {timeout} seconds\n"
        f.write(output + "\n")
    return output


def own_postgres_processes(processes):
    return [p for p in processes if p[1] == "postgres" and p[4] == os.getuid()]


def dump_thread_states(output, processes):
    """
    Writes the state and the kernel wait channel of all threads of the postgres
    processes. Unlike gdb, this doesn't need to attach to the processes.
    """
    with open(output / "threads.txt", "w") as f:
        for pid, _comm, cmdline, _sid, _uid in own_postgres_processes(processes):
            f.write(f"{pid}: {cmdline}\n")
            tasks = Path("/proc") / str(pid) / "task"
            try:
                tids = sorted(int(t.name) for t in tasks.iterdir())
            except FileNotFoundError:
                continue
            for tid in tids:
                task = tasks / str(tid)
                try:
                    name = (task / "comm").read_text().strip()
                    state = (task / "stat").read_text().rsplit(")", 1)[1].split()[0]
                    wchan = (task / "wchan").read_text().strip()
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    continue
                f.write(f"    {tid} {name}: state={state} wchan={wchan}\n")


def dump_postgres_stacks(output, processes):
    """Writes the stack traces of all threads of the postgres processes of this user"""
    if not shutil.which("gdb"):
        print(
            "gdb is not installed, can't dump the stack traces of the postgres processes"
        )
        return

    gdb = [
        "gdb",
        "-batch",
        "-nx",
        "-ex",
        "set pagination off",
        "-ex",
        "thread apply all bt",
        "-p",
    ]
    sudo = can_sudo()
    postgres = own_postgres_processes(processes)
    for pid, _comm, cmdline, _sid, _uid in postgres[:MAX_GDB_PROCESSES]:
        path = output / f"gdb-{pid}.txt"
        result = run_to_file(gdb + [str(pid)], path, cmdline, GDB_TIMEOUT)
        # pg_ctl detaches the servers from the command, so attaching to them can require root
        if "Could not attach" in result and sudo:
            run_to_file(
                ["sudo", "-n"] + gdb + [str(pid)],
                path,
                "retrying with sudo",
                GDB_TIMEOUT,
            )
    if len(postgres) > MAX_GDB_PROCESSES:
        print(
            f"Only dumped the stack traces of {MAX_GDB_PROCESSES} of {len(postgres)} postgres processes"
        )


ACTIVITY_SQL = """
SELECT pid, backend_type, state, wait_event_type, wait_event, now() - query_start AS running, left(query, 200) AS query
FROM pg_stat_activity ORDER BY pid;
SELECT pid, locktype, relation::regclass, mode, granted FROM pg_locks ORDER BY pid;
"""


def dump_postgres_activity(output, processes):
    """Writes pg_stat_activity and pg_locks of every postgres server"""
    if not shutil.which("psql"):
        print(
            "psql is not on the PATH, can't query the activity of the postgres servers"
        )
        return

    for _pid, _comm, cmdline, _sid, _uid in own_postgres_processes(processes):
        args = cmdline.split()
        # Only the postmaster still has its original command line
        if "-D" not in args or "-p" not in args[:-1]:
            continue
        port = args[args.index("-p") + 1]
        # pg_regress servers only listen on the socket directory given with -k
        hosts = ["127.0.0.1", "/tmp"]
        if "-k" in args[:-1]:
            hosts.insert(0, args[args.index("-k") + 1])
        env = dict(
            os.environ, PGOPTIONS="-c statement_timeout=10s", PGCONNECT_TIMEOUT="10"
        )
        path = output / f"activity-{port}.txt"
        connected = False
        for host in hosts:
            for user in (None, "postgres"):
                command = ["psql", "-X", "-h", host, "-p", port, "-d", "postgres"]
                if user:
                    command += ["-U", user]
                with open(path, "a") as f:
                    f.write(f"# {' '.join(command)}\n")
                    f.flush()
                    try:
                        result = subprocess.run(
                            command + ["-c", ACTIVITY_SQL],
                            stdout=f,
                            stderr=subprocess.STDOUT,
                            env=env,
                            timeout=30,
                        )
                        connected = result.returncode == 0
                    except subprocess.TimeoutExpired:
                        f.write("psql did not finish in 30 seconds\n")
                if connected:
                    break
            if connected:
                break


def dump_python_tracebacks(processes, session_id):
    """Makes the Python processes of the command print the tracebacks of all their threads to stderr"""
    pids = [p[0] for p in processes if p[3] == session_id and p[1].startswith("python")]
    for pid in pids:
        try:
            os.kill(pid, signal.SIGABRT)
        except ProcessLookupError:
            pass
    # Give them time to write the tracebacks
    time.sleep(5)


def copy_logs(output, logs):
    if not logs or not logs.exists():
        return
    for log in logs.rglob("*.log"):
        target = output / "logs" / log.relative_to(logs)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(log, target)


def kill_session(session_id):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(session_id, sig)
        except ProcessLookupError:
            return
        time.sleep(10)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--timeout", type=float, required=True, help="time limit in seconds"
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="directory for the diagnostics"
    )
    parser.add_argument(
        "--logs", type=Path, help="directory with log files to copy on timeout"
    )
    parser.add_argument("command", nargs="+")
    args = parser.parse_args()

    # In a new session, so we can find and kill all the processes of the command
    process = subprocess.Popen(args.command, start_new_session=True)
    try:
        return process.wait(timeout=args.timeout)
    except subprocess.TimeoutExpired:
        pass

    print(
        f"\n\n{' '.join(args.command)} did not finish in {args.timeout:g} seconds, collecting diagnostics in {args.output}"
    )
    sys.stdout.flush()
    args.output.mkdir(parents=True, exist_ok=True)
    processes = list(list_processes())
    with open(args.output / "processes.txt", "w") as f:
        for pid, comm, cmdline, session_id, uid in processes:
            f.write(f"{pid} sid={session_id} uid={uid} {comm}: {cmdline}\n")

    dump_thread_states(args.output, processes)
    dump_postgres_activity(args.output, processes)
    dump_postgres_stacks(args.output, processes)
    dump_python_tracebacks(processes, process.pid)
    copy_logs(args.output, args.logs)
    kill_session(process.pid)
    process.wait()
    print(f"Killed {' '.join(args.command)} after {args.timeout:g} seconds")
    return 124


if __name__ == "__main__":
    sys.exit(main())
