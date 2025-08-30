#!/bin/bash

set -euo pipefail

# Check arguments
if [ $# -ne 1 ] || [[ "$1" != PG_SRC=* ]]; then
    echo "Usage: $0 PG_SRC=/path/to/postgres"
    exit 1
fi

PG_SRC="${1#PG_SRC=}"

if [ ! -d "$PG_SRC" ]; then
    echo "Error: PostgreSQL source directory '$PG_SRC' does not exist"
    exit 1
fi

echo "Building PostgreSQL with Valgrind support..."

# Setup temp directories
TEMP_DIR=$(mktemp -d)
PG_INSTALL="$TEMP_DIR/postgres_install"
PG_DATA="$TEMP_DIR/postgres_data"
PG_LOG="$TEMP_DIR/postgres_log"

# Cleanup function
cleanup() {
    if [ -n "${PG_PID:-}" ]; then
        kill $PG_PID 2>/dev/null || true
    fi
    rm -rf "$TEMP_DIR"
}

trap cleanup EXIT

# Build PostgreSQL
cd "$PG_SRC"
./configure --prefix="$PG_INSTALL" --enable-debug \
    CFLAGS="-O0 -g -DUSE_VALGRIND" \
    CXXFLAGS="-O0 -g -DUSE_VALGRIND"
make -j$(nproc)
make install
cd - > /dev/null

# Build pg_duckdb
echo "Building pg_duckdb..."
make clean
make PG_CONFIG="$PG_INSTALL/bin/pg_config"

# Setup and start PostgreSQL
echo "Starting PostgreSQL..."
"$PG_INSTALL/bin/initdb" -D "$PG_DATA" --no-locale
"$PG_INSTALL/bin/pg_ctl" -D "$PG_DATA" -l "$PG_LOG/postgres.log" start
sleep 3

if ! "$PG_INSTALL/bin/pg_ctl" -D "$PG_DATA" status > /dev/null 2>&1; then
    echo "PostgreSQL failed to start"
    cat "$PG_LOG/postgres.log"
    exit 1
fi

PG_PID=$(cat "$PG_DATA/postmaster.pid" 2>/dev/null || echo "")

# Run Valgrind test
echo "Running Valgrind test..."
TEST_SQL="test/regression/sql/valgrind_basic.sql"

if [ ! -f "$TEST_SQL" ]; then
    echo "Test file '$TEST_SQL' not found"
    exit 1
fi

valgrind \
    --tool=memcheck \
    --leak-check=full \
    --show-leak-kinds=all \
    --track-origins=yes \
    --num-callers=50 \
    --error-exitcode=99 \
    --suppressions="$PG_SRC/src/tools/valgrind.supp" \
    "$PG_INSTALL/bin/psql" \
    -h localhost -p 5432 -U "$(whoami)" -d postgres \
    -f "$TEST_SQL"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Valgrind test passed"
else
    echo "Valgrind test failed"
fi

exit $EXIT_CODE
