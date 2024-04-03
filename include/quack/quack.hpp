#pragma once

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {
struct QuackWriteState;
} // namespace duckdb

extern "C" {

#include "postgres.h"

#include "storage/relfilelocator.h"
#include "access/tupdesc.h"
#include "access/tableam.h"
#include "utils/rel.h"
#include "common/relpath.h"

/* Quack GUC */
extern char *quack_data_dir;

// quack_hooks.c
extern void quack_init_hooks(void);

// quack_tableam.c
extern void quack_init_tableam(void);
const TableAmRoutine *quack_get_table_am_routine(void);

extern const char *quack_duckdb_type(Oid columnOid);

// quack_write_manager.c
extern duckdb::QuackWriteState *quack_init_write_state(Relation relation, Oid databaseOid,
                                                       SubTransactionId currentSubXid);
extern void quack_flush_write_state(SubTransactionId currentSubXid, SubTransactionId parentSubXid, bool commit);
// quack.c
void _PG_init(void);
}

namespace duckdb {

extern void quack_translate_value(TupleTableSlot *slot, Value &value, idx_t col);

// quack_utility.c
extern void quack_execute_query(const char *query);
extern unique_ptr<DuckDB> quack_open_database(Oid databaseOid, bool preserveInsertOrder);
extern unique_ptr<Connection> quack_open_connection(DuckDB database);
extern void quack_append_value(Appender &appender, Oid columnOid, Datum value);
extern unique_ptr<Appender> quack_create_appender(Connection &connection, const char *tableName);

typedef struct QuackWriteState {
	RelFileNumber rel_node;
	unique_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	unique_ptr<Appender> appender;
	uint16 row_count;
} QuackWriteState;

} // namespace duckdb
