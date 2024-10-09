#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "access/table.h"
#include "commands/copy.h"
#include "executor/executor.h"
#include "nodes/parsenodes.h"
#include "parser/parse_relation.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/lsyscache.h"
#include "parser/scanner.h"
#include "parser/gram.h"
#include "tcop/tcopprot.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
#include "pgduckdb/pgduckdb_ruleutils.h"
}

#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

static constexpr char s3_filename_prefix[] = "s3://";
static constexpr char gcs_filename_prefix[] = "gs://";
static constexpr char r2_filename_prefix[] = "r2://";

static int
FindQueryTokenOffset(const char *query, yytokentype token_to_match) {
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE yylloc;
	int token_offset = -1;
	int nested_level = 0;
	int tok;

	yyscanner = scanner_init(query, &yyextra, &ScanKeywords, ScanKeywordTokens);

	for (;;) {
		tok = core_yylex(&yylval, &yylloc, yyscanner);
		if (tok == 0) {
			break;
		} else if (tok == '(') {
			nested_level++;
		} else if (tok == ')') {
			nested_level--;
		} else if (tok == token_to_match && nested_level == 0) {
			token_offset = yylloc;
			break;
		}
	}
	scanner_finish(yyscanner);
	return token_offset;
}

static std::string
CreateRelationCopyString(ParseState *pstate, CopyStmt *copy_stmt, bool *allowed) {
	ParseNamespaceItem *nsitem;
#if PG_VERSION_NUM >= 160000
	RTEPermissionInfo *perminfo;
#else
	RangeTblEntry *rte;
#endif
	Relation rel;
	Oid relid;
	duckdb::string relation_copy;

	/* Open and lock the relation, using the appropriate lock type. */
	rel = table_openrv(copy_stmt->relation, AccessShareLock);
	relid = RelationGetRelid(rel);
	nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock, NULL, false, false);

#if PG_VERSION_NUM >= 160000
	perminfo = nsitem->p_perminfo;
	perminfo->requiredPerms = ACL_SELECT;
#else
	rte = nsitem->p_rte;
	rte->requiredPerms = ACL_SELECT;
#endif

#if PG_VERSION_NUM >= 160000
	if (!ExecCheckPermissions(pstate->p_rtable, list_make1(perminfo), false)) {
		ereport(WARNING,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("(PGDuckDB/CreateRelationCopyString) Failed Permission \"%s\"", RelationGetRelationName(rel))));
		*allowed = false;
	}
#else
	if (!ExecCheckRTPerms(pstate->p_rtable, true)) {
		ereport(WARNING,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("(PGDuckDB/CreateRelationCopyString) Failed Permission \"%s\"", RelationGetRelationName(rel))));
		*allowed = false;
	}
#endif

	table_close(rel, AccessShareLock);

	if(!*allowed) {
		return relation_copy;
	}

	/*
	 * RLS for relation. We should probably bail out at this point.
	 */
	if (check_enable_rls(relid, InvalidOid, false) == RLS_ENABLED) {
		ereport(WARNING,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("(PGDuckDB/CreateRelationCopyString) RLS enabled on \"%s\"", RelationGetRelationName(rel))));
		*allowed = false;
		return relation_copy;
	}

	relation_copy += pgduckdb_relation_name(relid);
	if (copy_stmt->attlist) {
		ListCell *lc;
		relation_copy += "(";
		bool first = true;
		foreach (lc, copy_stmt->attlist) {
			if (!first) {
				relation_copy += ", ";
			}
			first = false;
			relation_copy += strVal(lfirst(lc));
		}
		relation_copy += ") ";
	}

	*allowed = true;
	return relation_copy;
}

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

	int token_start_offset = FindQueryTokenOffset(query_string, TO);
	duckdb::string rewritten_query_string;

	if (copy_stmt->query) {
		List *rewritten;
		RawStmt *raw_stmt;
		Query *query;

		raw_stmt = makeNode(RawStmt);
		raw_stmt->stmt = copy_stmt->query;
		raw_stmt->stmt_location = pstmt->stmt_location;
		raw_stmt->stmt_len = pstmt->stmt_len;

		rewritten = pg_analyze_and_rewrite_fixedparams(raw_stmt, query_string, NULL, 0, NULL);
		query = linitial_node(Query, rewritten);
		rewritten_query_string = duckdb::StringUtil::Format("COPY (%s) %s;", pgduckdb_pg_get_querydef(query, false),
		                                                    query_string + token_start_offset);
	} else {
		bool copy_allowed = true;
		ParseState *pstate = make_parsestate(NULL);
		pstate->p_sourcetext = query_string;
		pstate->p_queryEnv = query_env;
		std::string relation_copy_part = CreateRelationCopyString(pstate, copy_stmt, &copy_allowed);
		if (!copy_allowed) {
			return false;
		}
		rewritten_query_string =
		    duckdb::StringUtil::Format("COPY %s %s;", relation_copy_part, query_string + token_start_offset);
	}

	auto res = pgduckdb::DuckDBQueryOrThrow(rewritten_query_string);
	auto chunk = res->Fetch();
	*processed = chunk->GetValue(0, 0).GetValue<uint64_t>();
	return true;
}
