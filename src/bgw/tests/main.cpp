extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "utils/builtins.h"
}

#include "pgduckdb/bgw/tests/main.hpp"

std::vector<std::pair<std::string, std::function<void()>>>
select_tests(const std::string &needle) {
	std::vector<std::pair<std::string, std::function<void()>>> result;
	for (const auto &[test_name, test_func] : pgduckdb_tests::PGDUCKDB_TESTS) {
		if (test_name.find(needle) != std::string::npos) {
			result.push_back({test_name, test_func});
		}
	}

	return result;
}

extern "C" {

PG_FUNCTION_INFO_V1(run_pgduckdb_tests);

Datum
run_pgduckdb_tests(PG_FUNCTION_ARGS) {
	char *message = text_to_cstring(PG_GETARG_TEXT_PP(0));
	FuncCallContext *funcctx;
	TupleDesc tupdesc;
	Datum result;
	Datum values[3];
	bool nulls[3] = {false, false, false};
	HeapTuple tuple;

	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		// Initialize function call context
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		// Define a tuple descriptor for the return type (two columns)
		tupdesc = CreateTemplateTupleDesc(3);
		TupleDescInitEntry(tupdesc, (AttrNumber)1, "id", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber)2, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber)3, "success", BOOLOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	const auto call_cntr = funcctx->call_cntr;
	const auto tests = strlen(message) > 0 ? select_tests(message) : pgduckdb_tests::PGDUCKDB_TESTS;
	if (call_cntr >= tests.size()) {
		SRF_RETURN_DONE(funcctx);
	}

	const auto &[test_name, test_func] = tests[call_cntr];

	bool success = false;
	MemoryContext ccxt = CurrentMemoryContext;
	PG_TRY();
	{
		try {
			test_func();
			success = true;
			elog(DEBUG1, "Test #%lu '%s' succeeded.", call_cntr + 1, test_name.c_str());
		} catch (const std::exception &e) {
			elog(INFO, "Test #%lu '%s' failed (STD exception): %s", call_cntr + 1, test_name.c_str(), e.what());
		}
	}
	PG_CATCH();
	{
		MemoryContext ecxt = MemoryContextSwitchTo(ccxt);
		{
			const auto err_data = CopyErrorData();
			elog(INFO, "Test #%lu '%s' failed (PG CATCH): %s", call_cntr + 1, test_name.c_str(), err_data->message);
			FreeErrorData(err_data);
		}
		MemoryContextSwitchTo(ecxt);
		FlushErrorState();
	}
	PG_END_TRY();

	values[0] = Int32GetDatum(call_cntr + 1);
	values[1] = CStringGetTextDatum(test_name.c_str());
	values[2] = BoolGetDatum(success);

	tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	SRF_RETURN_NEXT(funcctx, result);
}
}