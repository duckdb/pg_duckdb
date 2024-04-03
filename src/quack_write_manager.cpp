#include "quack/quack.hpp"

extern "C" {

#include "postgres.h"

#include "access/heaptoast.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "utils/relmapper.h"

static HTAB *quack_write_state_map = NULL;
static MemoryContext quack_write_state_context = NULL;

typedef struct SubXidWriteState {
	SubTransactionId subXid;
	duckdb::QuackWriteState *quack_write_state_entry;
	struct SubXidWriteState *next;
} SubXidWriteState;

typedef struct QuackWriteStateMapEntry {
	Oid rel_node;
	SubXidWriteState *write_state_stack;
} QuackWriteStateMapEntry;

static MemoryContextCallback cleanup_callback;

static void cleanup_write_state_map(void *arg) {
	quack_write_state_map = NULL;
	quack_write_state_context = NULL;
}

void quack_flush_write_state(SubTransactionId currentSubXid, SubTransactionId parentSubXid, bool commit) {
	HASH_SEQ_STATUS status;
	QuackWriteStateMapEntry *entry;

	if (quack_write_state_map == NULL) {
		return;
	}

	hash_seq_init(&status, quack_write_state_map);

	while ((entry = (QuackWriteStateMapEntry *)hash_seq_search(&status)) != 0) {
		SubXidWriteState *stack_head = entry->write_state_stack;

		if (entry->write_state_stack == NULL) {
			continue;
		}

		if (stack_head->subXid == currentSubXid) {
			if (commit) {
				duckdb::QuackWriteState *quack_write_state = stack_head->quack_write_state_entry;
				quack_write_state->appender->Close();
				quack_write_state->appender.reset();
				quack_write_state->database.reset();
				quack_write_state->connection.reset();
			}

			entry->write_state_stack = stack_head->next;
		}
	}
}
}

duckdb::QuackWriteState *quack_init_write_state(Relation relation, Oid databaseOid, SubTransactionId currentSubXid) {
	bool found;
	QuackWriteStateMapEntry *hash_entry = NULL;
	SubXidWriteState *stack_entry = NULL;
	duckdb::QuackWriteState *quack_write_state = NULL;
	MemoryContext oldContext = NULL;

	if (quack_write_state_map == NULL) {
		HASHCTL info;
		uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

		quack_write_state_context =
		    AllocSetContextCreate(TopTransactionContext, "Quack Write State context", ALLOCSET_DEFAULT_SIZES);

		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(Oid);
		info.hash = oid_hash;
		info.entrysize = sizeof(QuackWriteStateMapEntry);
		info.hcxt = quack_write_state_context;

		quack_write_state_map = hash_create("quack cache map", 64, &info, hashFlags);

		cleanup_callback.arg = NULL;
		cleanup_callback.func = &cleanup_write_state_map;
		cleanup_callback.next = NULL;
		MemoryContextRegisterResetCallback(quack_write_state_context, &cleanup_callback);
	}

	// FIXME: not sure what the effects of the 'shared' parameter are
	auto fileNumber = RelationMapOidToFilenumber(relation->rd_id, /*shared=*/false);
	hash_entry = (QuackWriteStateMapEntry *)hash_search(quack_write_state_map, &fileNumber, HASH_ENTER, &found);

	if (!found) {
		hash_entry->write_state_stack = NULL;
	}

	if (hash_entry->write_state_stack != NULL) {
		SubXidWriteState *stackHead = hash_entry->write_state_stack;

		if (stackHead->subXid == currentSubXid) {
			return stackHead->quack_write_state_entry;
		}
	}

	oldContext = MemoryContextSwitchTo(quack_write_state_context);

	if (stack_entry == NULL) {
		stack_entry = (SubXidWriteState *)palloc0(sizeof(SubXidWriteState));
		stack_entry->subXid = currentSubXid;
		stack_entry->next = hash_entry->write_state_stack;
		hash_entry->write_state_stack = stack_entry;
	}

	quack_write_state = (duckdb::QuackWriteState *)palloc0(sizeof(duckdb::QuackWriteState));

	quack_write_state->rel_node = RelationMapOidToFilenumber(relation->rd_id, false);
	quack_write_state->database = duckdb::quack_open_database(databaseOid, true);
	quack_write_state->connection = quack_open_connection(*quack_write_state->database);
	quack_write_state->appender = quack_create_appender(*quack_write_state->connection, relation->rd_rel->relname.data);
	quack_write_state->row_count = 0;

	stack_entry->quack_write_state_entry = quack_write_state;

	MemoryContextSwitchTo(oldContext);

	return quack_write_state;
}
