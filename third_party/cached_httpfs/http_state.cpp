#include "http_state.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {


void HTTPState::Reset() {
	// Reset Counters
	head_count = 0;
	get_count = 0;
	put_count = 0;
	post_count = 0;
	total_bytes_received = 0;
	total_bytes_sent = 0;
}

shared_ptr<HTTPState> HTTPState::TryGetState(ClientContext &context) {
	return context.registered_state->GetOrCreate<HTTPState>("http_state");
}

shared_ptr<HTTPState> HTTPState::TryGetState(optional_ptr<FileOpener> opener) {
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (client_context) {
		return TryGetState(*client_context);
	}
	return nullptr;
}

void HTTPState::WriteProfilingInformation(std::ostream &ss) {
	string read = "in: " + StringUtil::BytesToHumanReadableString(total_bytes_received);
	string written = "out: " + StringUtil::BytesToHumanReadableString(total_bytes_sent);
	string head = "#HEAD: " + to_string(head_count);
	string get = "#GET: " + to_string(get_count);
	string put = "#PUT: " + to_string(put_count);
	string post = "#POST: " + to_string(post_count);

	constexpr idx_t TOTAL_BOX_WIDTH = 39;
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││" + QueryProfiler::DrawPadded("HTTPFS HTTP Stats", TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││                                   ││\n";
	ss << "││" + QueryProfiler::DrawPadded(read, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(written, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(head, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(get, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(put, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(post, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
}

} // namespace duckdb
