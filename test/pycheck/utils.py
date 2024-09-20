import subprocess
from contextlib import closing, contextmanager
from pathlib import Path

from contextlib import asynccontextmanager

import asyncio
import os
import platform
import re
import shlex
import socket
import sys
import time
import typing
from typing import Any
from tempfile import gettempdir

import filelock
import psycopg
import psycopg.sql
import psycopg.conninfo
from psycopg import sql

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
os.chdir(TEST_DIR)

PGDATA = TEST_DIR / "pgdata"
PGHOST = "127.0.0.1"

if os.name == "nt":
    USE_UNIX_SOCKETS = False
    HAVE_GETPEEREID = False

    # psycopg only supports WindowsSelectorEventLoopPolicy
    from asyncio import WindowsSelectorEventLoopPolicy

    asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    WINDOWS = True
else:
    USE_UNIX_SOCKETS = True
    HAVE_GETPEEREID = True
    WINDOWS = False

USE_SUDO = bool(os.environ.get("USE_SUDO"))

LINUX = False
MACOS = False
FREEBSD = False
OPENBSD = False

if platform.system() == "Linux":
    LINUX = True
elif platform.system() == "Darwin":
    MACOS = True
elif platform.system() == "FreeBSD":
    FREEBSD = True
elif platform.system() == "OpenBSD":
    OPENBSD = True

BSD = MACOS or FREEBSD or OPENBSD


def eprint(*args, **kwargs):
    """eprint prints to stderr"""

    print(*args, file=sys.stderr, **kwargs)


def run(command, *args, check=True, shell=None, silent=False, **kwargs):
    """run runs the given command and prints it to stderr"""

    if shell is None:
        shell = isinstance(command, str)

    if not shell:
        command = list(map(str, command))

    if not silent:
        if shell:
            eprint(f"+ {command}")
        else:
            # We could normally use shlex.join here, but it's not available in
            # Python 3.6 which we still like to support
            unsafe_string_cmd = " ".join(map(shlex.quote, command))
            eprint(f"+ {unsafe_string_cmd}")
    if silent:
        kwargs.setdefault("stdout", subprocess.DEVNULL)
    return subprocess.run(command, *args, check=check, shell=shell, **kwargs)


def sudo(command, *args, shell=None, **kwargs):
    """
    A version of run that prefixes the command with sudo when the process is
    not already run as root
    """
    effective_user_id = os.geteuid()

    if effective_user_id == 0:
        return run(command, *args, shell=shell, **kwargs)

    if shell is None:
        shell = isinstance(command, str)

    if shell:
        return run(f"sudo {command}", *args, shell=shell, **kwargs)
    else:
        return run(["sudo", *command], *args, shell=shell, **kwargs)


def capture(command, *args, stdout=subprocess.PIPE, encoding="utf-8", **kwargs):
    return run(command, *args, stdout=stdout, encoding=encoding, **kwargs).stdout


def get_pg_major_version():
    full_version_string = capture("initdb --version", silent=True)
    major_version_string = re.search("[0-9]+", full_version_string)
    assert major_version_string is not None
    return int(major_version_string.group(0))


PG_MAJOR_VERSION = get_pg_major_version()

# this is out of ephemeral port range for many systems hence
# it is a lower change that it will conflict with "in-use" ports
PORT_LOWER_BOUND = 10200

# ephemeral port start on many Linux systems
PORT_UPPER_BOUND = 32768

next_port = PORT_LOWER_BOUND


class NoResultClass:
    def __eq__(self, other):
        return self is other

    def __nonzero__(self):
        return False

    pass


NoResult = NoResultClass()


def cleanup_test_leftovers(*nodes):
    """
    Cleaning up test leftovers needs to be done in a specific order, because
    some of these leftovers depend on others having been removed. They might
    even depend on leftovers on other nodes being removed. So this takes a list
    of nodes, so that we can clean up all test leftovers globally in the
    correct order.
    """
    for node in nodes:
        node.cleanup_subscriptions()

    for node in nodes:
        node.cleanup_publications()

    for node in nodes:
        node.cleanup_replication_slots()

    for node in nodes:
        node.cleanup_schemas()

    for node in nodes:
        node.cleanup_users()


class PortLock:
    def __init__(self):
        global next_port
        while True:
            next_port += 1
            if next_port >= PORT_UPPER_BOUND:
                next_port = PORT_LOWER_BOUND

            self.lock = filelock.FileLock(Path(gettempdir()) / f"port-{next_port}.lock")
            try:
                self.lock.acquire(timeout=0)
            except filelock.Timeout:
                continue

            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                try:
                    s.bind(("127.0.0.1", next_port))
                    self.port = next_port
                    break
                except Exception:
                    continue

    def release(self):
        self.lock.release()


def notice_handler(diag: psycopg.errors.Diagnostic):
    print(f"{diag.severity}: {diag.message_primary}")
    if diag.message_detail:
        print(f"DETAIL: {diag.message_detail}")
    if diag.message_hint:
        print(f"HINT: {diag.message_hint}")
    if diag.context:
        print(f"CONTEXT: {diag.context}")


def simplify_query_results(results) -> Any:
    """
    Simplify the results of a query so that the caller doesn't have to unpack
    arrays and tuples of length 1.
    """
    if len(results) == 1:
        row = results[0]
        if len(row) == 1:
            # If there's only a single cell, just return the value
            return row[0]
        # If there's only a single row, just return the that row
        return row

    if len(results) != 0 and len(results[0]) == 1:
        # If there's only a single column, return an array of values
        return [row[0] for row in results]

    # if there are multiple rows and columns, return the results as is
    return results


# Monkey-patch an easy to use sql method into psycopg its Cursor classes


class Cursor:
    """This is a wrapper around psycopg.Cursor that adds a sql method"""

    def __init__(self, cursor: psycopg.Cursor):
        self.cursor = cursor

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def sql(self, query, params=None, **kwargs) -> Any:
        self.execute(query, params, **kwargs)
        try:
            return simplify_query_results(self.fetchall())
        except psycopg.ProgrammingError as e:
            if "the last operation didn't produce a result" == str(e):
                return NoResult
            raise


class AsyncCursor:
    """This is a wrapper around psycopg.AsyncCursor that adds a sql method"""

    def __init__(self, cursor: psycopg.AsyncCursor):
        self.cursor = cursor

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def sql(self, query, params=None, **kwargs):
        return asyncio.ensure_future(self.sql_coroutine(query, params, **kwargs))

    async def sql_coroutine(self, query, params=None, **kwargs) -> Any:
        await self.execute(query, params, **kwargs)
        try:
            return simplify_query_results(await self.fetchall())
        except psycopg.ProgrammingError as e:
            if "the last operation didn't produce a result" == str(e):
                return NoResult
            raise


class Postgres:
    def __init__(self, pgdata):
        self.pgdata = pgdata
        self.log_path = self.pgdata / "pg.log"
        self.restarted = False
        self.reloaded = False

        self.port_lock = PortLock()
        self.host = "127.0.0.1"
        self.port = self.port_lock.port
        self.default_db = "postgres"
        self.default_user = "postgres"

        # Used to track objects that we want to clean up at the end of a test
        self.subscriptions = set()
        self.publications = set()
        self.replication_slots = set()
        self.schemas = set()
        self.users = set()

        self.cached_cur = None
        self.cached_acur = None
        self.search_path = None

    def set_default_connection_options(self, options):
        """Sets the default connection options on the given options dictionary"""
        options.setdefault("dbname", self.default_db)
        options.setdefault("user", self.default_user)
        options.setdefault("host", self.host)
        options.setdefault("port", self.port)
        options.setdefault("connect_timeout", 3)
        options.setdefault("client_encoding", "UTF8")
        if self.search_path:
            escaped_search_path = self.search_path.replace(" ", "\\ ")
            options.setdefault("options", f"-c search_path={escaped_search_path}")
        return options

    def make_conninfo(self, **kwargs) -> str:
        self.set_default_connection_options(kwargs)
        return psycopg.conninfo.make_conninfo(**kwargs)

    def disconnect(self):
        if self.cached_cur is not None:
            self.cached_cur.close()
            self.cached_cur = None

        if self.cached_acur is not None:
            self.cached_acur.close()
            self.cached_acur = None

    def conn(self, *, autocommit=True, **kwargs):
        """Open a psycopg connection to this server"""
        self.set_default_connection_options(kwargs)
        conn = psycopg.connect(
            autocommit=autocommit,
            **kwargs,
        )
        conn.add_notice_handler(notice_handler)
        return conn

    def aconn(self, *, autocommit=True, **kwargs):
        """Open an asynchronous psycopg connection to this server"""
        self.set_default_connection_options(kwargs)
        return psycopg.AsyncConnection.connect(
            autocommit=autocommit,
            **kwargs,
        )

    @contextmanager
    def cur(self, autocommit=True, **kwargs):
        """Open an psycopg cursor to this server

        The connection and the cursors automatically close once you leave the
        "with" block
        """
        with self.conn(
            autocommit=autocommit,
            **kwargs,
        ) as conn:
            with conn.cursor() as cur:
                yield Cursor(cur)

    @asynccontextmanager
    async def acur(self, **kwargs):
        """Open an asynchronous psycopg cursor to this server

        The connection and the cursors automatically close once you leave the
        "async with" block
        """
        async with await self.aconn(**kwargs) as conn:
            async with conn.cursor() as cur:
                yield AsyncCursor(cur)

    def sql(self, query, params=None, **kwargs) -> Any:
        """Run an SQL query

        This opens a new connection and closes it once the query is done.
        """
        with self.cur(**kwargs) as cur:
            return cur.sql(query, params=params)

    def asql(self, query, **kwargs):
        """Run an SQL query in asynchronous task

        This opens a new connection and closes it once the query is done
        """
        return asyncio.ensure_future(self.asql_coroutine(query, **kwargs))

    async def asql_coroutine(self, query, params=None, **kwargs) -> Any:
        async with self.acur(**kwargs) as cur:
            return await cur.sql_coroutine(query, params=params)

    def psql(self, query, **kwargs):
        """Run an SQL query using psql instead of psycopg

        This opens a new connection and closes it once the query is done
        """

        self.set_default_connection_options(kwargs)
        connect_options = " ".join([f"{k}={v}" for k, v in kwargs.items()])

        run(["psql", f"port={self.port} {connect_options}", "-c", query], shell=False)

    @contextmanager
    def transaction(self, **kwargs):
        with self.cur(**kwargs) as cur:
            with cur.connection.transaction():
                yield cur

    def sleep(self, duration=3, **kwargs):
        """Run pg_sleep"""
        return self.sql(f"select pg_sleep({duration})", **kwargs)

    def asleep(self, duration=3, times=1, sequentially=False, **kwargs):
        """Run pg_sleep asynchronously in a task.

        times:
            You can create a single task that opens multiple connections, which
            run pg_sleep concurrently. The asynchronous task will only complete
            once all these pg_sleep calls are finished.
        sequentially:
            Instead of running all pg_sleep calls spawned by providing
            times > 1 concurrently, this will run them sequentially.
        """
        return asyncio.ensure_future(
            self.asleep_coroutine(
                duration=duration, times=times, sequentially=sequentially, **kwargs
            )
        )

    async def asleep_coroutine(self, duration=3, times=1, sequentially=False, **kwargs):
        """This is the coroutine that the asleep task runs internally"""
        if not sequentially:
            await asyncio.gather(
                *[
                    self.asql(f"select pg_sleep({duration})", **kwargs)
                    for _ in range(times)
                ]
            )
        else:
            for _ in range(times):
                await self.asql(f"select pg_sleep({duration})", **kwargs)

    def create_user(self, name, args: typing.Optional[psycopg.sql.Composable] = None):
        self.users.add(name)
        if args is None:
            args = sql.SQL("")
        self.sql(sql.SQL("CREATE USER {} {}").format(sql.Identifier(name), args))

    def create_schema(self, name, dbname=None):
        dbname = dbname or self.default_db
        self.schemas.add((dbname, name))
        self.sql(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))

    def create_publication(self, name: str, args: psycopg.sql.Composable, dbname=None):
        dbname = dbname or self.default_db
        self.publications.add((dbname, name))
        self.sql(sql.SQL("CREATE PUBLICATION {} {}").format(sql.Identifier(name), args))

    def create_logical_replication_slot(self, name, plugin):
        self.replication_slots.add(name)
        self.sql(
            "SELECT pg_catalog.pg_create_logical_replication_slot(%s,%s)",
            (name, plugin),
        )

    def create_physical_replication_slot(self, name):
        self.replication_slots.add(name)
        self.sql(
            "SELECT pg_catalog.pg_create_physical_replication_slot(%s)",
            (name,),
        )

    def create_subscription(self, name: str, args: psycopg.sql.Composable, dbname=None):
        dbname = dbname or self.default_db
        self.subscriptions.add((dbname, name))
        self.sql(
            sql.SQL("CREATE SUBSCRIPTION {} {}").format(sql.Identifier(name), args)
        )

    def cleanup_users(self):
        for user in self.users:
            self.sql(sql.SQL("DROP USER IF EXISTS {}").format(sql.Identifier(user)))

    def cleanup_schemas(self):
        for dbname, schema in self.schemas:
            self.sql(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                ),
                dbname=dbname,
            )

    def cleanup_publications(self):
        for dbname, publication in self.publications:
            self.sql(
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(
                    sql.Identifier(publication)
                ),
                dbname=dbname,
            )

    def cleanup_replication_slots(self):
        for slot in self.replication_slots:
            start = time.time()
            while True:
                try:
                    self.sql(
                        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                        (slot,),
                    )
                except psycopg.errors.ObjectInUse:
                    if time.time() < start + 10:
                        time.sleep(0.5)
                        continue
                    raise
                break

    def cleanup_subscriptions(self):
        for dbname, subscription in self.subscriptions:
            try:
                self.sql(
                    sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(
                        sql.Identifier(subscription)
                    ),
                    dbname=dbname,
                )
            except psycopg.errors.UndefinedObject:
                # Subscription didn't exist already
                continue
            self.sql(
                sql.SQL("ALTER SUBSCRIPTION {} SET (slot_name = NONE)").format(
                    sql.Identifier(subscription)
                ),
                dbname=dbname,
            )
            self.sql(
                sql.SQL("DROP SUBSCRIPTION {}").format(sql.Identifier(subscription)),
                dbname=dbname,
            )

    def debug(self):
        print("Connect manually to:\n   ", repr(self.make_conninfo()))
        print("Press Enter to continue running the test...")
        input()

    def psql_debug(self, **kwargs):
        conninfo = self.make_conninfo(**kwargs)
        run(
            ["psql", conninfo],
            silent=True,
        )

    def initdb(self):
        run(
            f"initdb -A trust --nosync --username postgres --pgdata {self.pgdata}",
            stdout=subprocess.DEVNULL,
        )

        with self.conf_path.open(mode="a") as pgconf:
            if USE_UNIX_SOCKETS:
                pgconf.write("unix_socket_directories = '/tmp'\n")
            pgconf.write("logging_collector = off\n")

            # Allow CREATE SUBSCRIPTION to work
            pgconf.write("wal_level = 'logical'\n")
            # Faster logical replication status update so tests with logical replication
            # run faster
            pgconf.write("wal_receiver_status_interval = 1\n")

            # Faster logical replication apply worker launch so tests with logical
            # replication run faster. This is used in ApplyLauncherMain in
            # src/backend/replication/logical/launcher.c.
            pgconf.write("wal_retrieve_retry_interval = '250ms'\n")

            # Make sure there's enough logical replication resources for our
            # tests
            if PG_MAJOR_VERSION >= 10:
                pgconf.write("max_logical_replication_workers = 5\n")
            pgconf.write("max_wal_senders = 5\n")
            pgconf.write("max_replication_slots = 10\n")
            pgconf.write("max_worker_processes = 20\n")

            # We need to make the log go to stderr so that the tests can
            # check what is being logged.  This should be the default, but
            # some packagings change the default configuration.
            pgconf.write("log_destination = stderr\n")
            # This makes tests run faster and we don't care about crash safety
            # of our test data.
            pgconf.write("fsync = false\n")

            # Use a consistent value across postgres versions, so test results
            # are the same.
            pgconf.write("extra_float_digits = 1\n")

            # Make sure this is consistent across platforms
            pgconf.write("datestyle = 'iso, dmy'\n")

            # And finally, enable pg_duckdb
            pgconf.write("shared_preload_libraries = pg_duckdb\n")
            pgconf.write("duckdb.execution = 'true'\n")

    def pgctl(self, command, **kwargs):
        run(f"pg_ctl -w --pgdata {self.pgdata} {command}", **kwargs)

    def apgctl(self, command, **kwargs):
        return asyncio.create_subprocess_shell(
            f"pg_ctl -w --pgdata {self.pgdata} {command}", **kwargs
        )

    def start(self):
        try:
            self.pgctl(f'-o "-p {self.port}" -l {self.log_path} start')
        except Exception:
            print("\n\nPG_LOG\n")
            with self.log_path.open() as f:
                print(f.read())
            raise

    def stop(self):
        self.pgctl("-m fast stop", check=False)

    def cleanup(self):
        self.stop()
        self.port_lock.release()

    def restart(self):
        self.restarted = True
        self.stop()
        self.start()

    def reset(self):
        os.truncate(self.pgdata / "postgresql.auto.conf", 0)

        # If a previous test restarted postgres, it was probably because of some
        # config that could only be changed across restarts. To reset those, we'll
        # have to restart it again. In other cases a reload should be enough to
        # reset the configuration.
        if self.restarted:
            self.restart()
            self.restarted = False
            self.reloaded = False
        elif self.reloaded:
            self.reload()
            self.reloaded = False

    def reload(self):
        if WINDOWS:
            # SIGHUP and thus reload don't exist on Windows
            self.restart()
        else:
            self.pgctl("reload")
        time.sleep(1)

    async def arestart(self):
        process = await self.apgctl("-m fast restart")
        await process.communicate()

    @property
    def hba_path(self):
        return self.pgdata / "pg_hba.conf"

    @property
    def conf_path(self):
        return self.pgdata / "postgresql.conf"

    def configure(self, config):
        """Configure specific Postgres settings using ALTER SYSTEM SET

        NOTE: after configuring a call to reload or restart is needed for the
        settings to become effective.
        """
        self.sql(f"alter system set {config}")

    @contextmanager
    def log_contains(self, re_string, times=None):
        """Checks if during this with block the log matches re_string

        re_string:
            The regex to search for.
        times:
            If None, any number of matches is accepted. If a number, only that
            specific number of matches is accepted.
        """
        with self.log_path.open() as f:
            f.seek(0, os.SEEK_END)
            yield
            content = f.read()
            if times is None:
                assert re.search(re_string, content)
            else:
                match_count = len(re.findall(re_string, content))
                assert match_count == times

    def cleanup_test_leftovers(self):
        cleanup_test_leftovers(self)
