#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "tcop/tcopprot.h"
#include "optimizer/optimizer.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/lsyscache.h"
}

#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

static constexpr char s3_filename_prefix[] = "s3://";
static constexpr char gcs_filename_prefix[] = "gs://";
static constexpr char r2_filename_prefix[] = "r2://";

bool
DuckdbCopy(PlannedStmt *pstmt, const char *query_string, struct QueryEnvironment *query_env, uint64 *processed) {
	CopyStmt *copy_stmt = (CopyStmt *)pstmt->utilityStmt;

	/* Copy `filename` should start with S3/GS/R2 prefix */
	if (duckdb::string(copy_stmt->filename).rfind(s3_filename_prefix, 0) &&
	    duckdb::string(copy_stmt->filename).rfind(gcs_filename_prefix, 0) &&
	    duckdb::string(copy_stmt->filename).rfind(r2_filename_prefix, 0)) {
		return false;
	}

	/* We handle only COPY .. TO */
	if (copy_stmt->is_from) {
		return false;
	}

	auto res = pgduckdb::DuckDBQueryOrThrow(query_string);
	auto chunk = res->Fetch();
	*processed = chunk->GetValue(0, 0).GetValue<uint64_t>();
	return true;
}
