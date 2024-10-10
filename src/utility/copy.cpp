#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "access/table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "tcop/tcopprot.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
#include "pgduckdb/pgduckdb_ruleutils.h"
}

#include "pgduckdb/utility/copy.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/vendor/pg_list.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

#include <functional>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

static constexpr char s3_filename_prefix[] = "s3://";
static constexpr char gcs_filename_prefix[] = "gs://";
static constexpr char r2_filename_prefix[] = "r2://";

typedef struct DuckdbCopyOptions {
	bool csv_mode;
	struct CSVOptions {
		char *delimiter;
		char *null_str;
		int null_str_len;
		bool include_header;
		char *quote;
		char *escape;
		List *force_quote;
		bool force_quote_all;
		bool *force_quote_flags;
	} csv_options;
} DuckdbCopyOptions;

/*
 * Returns the relation of the copy_stmt as a fully qualified DuckDB table reference. This is done
 * including the column names if provided in the original copy_stmt, e.g. my_table(column1, column2).
 * This also checks permissions on the table to see if the user is allowed to copy the data from this table.
 */
static duckdb::string
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

	if (!*allowed) {
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
			relation_copy += quote_identifier(strVal(lfirst(lc)));
		}
		relation_copy += ") ";
	}

	*allowed = true;
	return relation_copy;
}

static void
ReportConflictingCopyOption(DefElem *defel, bool *options_valid) {
	elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) Conflicting \'%s\' copy option.", defel->defname);
	*options_valid = false;
}

static duckdb::string
CheckAndCreateCopyOptions(CopyStmt *copy_stmt, bool *options_valid) {
	duckdb::string options_string;
	bool format_specified = false;
	bool header_specified = false;
	duckdb::vector<duckdb::string> options_parts;
	DuckdbCopyOptions duckdb_copy_options;
	ListCell *lc;

	memset(&duckdb_copy_options, 0, sizeof(DuckdbCopyOptions));

	foreach (lc, copy_stmt->options) {
		DefElem *defel = lfirst_node(DefElem, lc);

		if (strcmp(defel->defname, "format") == 0) {
			char *fmt = defGetString(defel);

			if (format_specified) {
				ReportConflictingCopyOption(defel, options_valid);
				break;
			}
			if (strcmp(fmt, "csv") == 0) {
				duckdb_copy_options.csv_mode = true;
			} else {
				elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY FORMAT \"%s\" not recognized", fmt);
				*options_valid = false;
				break;
			}
			format_specified = true;
		} else if (strcmp(defel->defname, "delimiter") == 0) {
			if (duckdb_copy_options.csv_options.delimiter) {
				ReportConflictingCopyOption(defel, options_valid);
				break;
			}
			duckdb_copy_options.csv_options.delimiter = defGetString(defel);
		} else if (strcmp(defel->defname, "null") == 0) {
			if (duckdb_copy_options.csv_options.null_str) {
				ReportConflictingCopyOption(defel, options_valid);
				break;
			}
			duckdb_copy_options.csv_options.null_str = defGetString(defel);
		} else if (strcmp(defel->defname, "header") == 0) {
			if (header_specified) {
				ReportConflictingCopyOption(defel, options_valid);
				break;
			}
			header_specified = true;
			bool option_header_parse_error = false;
			/* We need to catch ERROR that can be raised */
			// clang-format off
			PG_TRY();
			{
				duckdb_copy_options.csv_options.include_header = defGetBoolean(defel);
			}
			PG_CATCH();
			{
				option_header_parse_error = true;
			}
			PG_END_TRY();
			// clang-format on
			if (option_header_parse_error) {
				elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) %s requires a Boolean value", defel->defname);
				*options_valid = false;
				break;
			}
		} else if (strcmp(defel->defname, "quote") == 0) {
			if (duckdb_copy_options.csv_options.quote) {
				ReportConflictingCopyOption(defel, options_valid);
				break;
			}
			duckdb_copy_options.csv_options.quote = defGetString(defel);
		} else if (strcmp(defel->defname, "escape") == 0) {
			if (duckdb_copy_options.csv_options.escape) {
				ReportConflictingCopyOption(defel, options_valid);
				break;
			}
			duckdb_copy_options.csv_options.quote = defGetString(defel);
		} else if (strcmp(defel->defname, "force_quote") == 0) {
			if (duckdb_copy_options.csv_options.force_quote || duckdb_copy_options.csv_options.force_quote_all) {
				ReportConflictingCopyOption(defel, options_valid);
				break;
			}
			if (defel->arg && IsA(defel->arg, List)) {
				duckdb_copy_options.csv_options.force_quote = castNode(List, defel->arg);
			} else {
				elog(WARNING,
				     "(PGDuckDB/CheckAndCreateCopyOptions) Argument to option \"%s\" must be a "
				     "list of column names",
				     defel->defname);
				*options_valid = false;
				break;
			}
		} else {
			elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) Option \"%s\" not recognized", defel->defname);
			*options_valid = false;
			break;
		}
	}

	if (!*options_valid) {
		return options_string;
	}

	/* FORMAT */

	if (duckdb_copy_options.csv_mode) {
		options_parts.push_back("FORMAT CSV");
	}

	/* DELIMITER */

	if (!duckdb_copy_options.csv_mode && duckdb_copy_options.csv_options.delimiter != NULL) {
		elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY delimiter available only in CSV mode");
		*options_valid = false;
		return options_string;
	}

	if (duckdb_copy_options.csv_options.delimiter) {
		if (strlen(duckdb_copy_options.csv_options.delimiter) != 1) {
			elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY delimiter must be a single one-byte character");
			*options_valid = false;
			return options_string;
		}

		if ((strchr(duckdb_copy_options.csv_options.delimiter, '\r') != NULL ||
		     strchr(duckdb_copy_options.csv_options.delimiter, '\n') != NULL)) {
			elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY delimiter cannot be newline or carriage return");
			*options_valid = false;
			return options_string;
		}

		if (duckdb_copy_options.csv_options.delimiter &&
		    strchr("\\.abcdefghijklmnopqrstuvwxyz0123456789", duckdb_copy_options.csv_options.delimiter[0]) != NULL) {
			elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY delimiter cannot be \"%s\"",
			     duckdb_copy_options.csv_options.delimiter);
			*options_valid = false;
			return options_string;
		}

		options_parts.push_back(
		    duckdb::StringUtil::Format("DELIMITER \'%c\'", duckdb_copy_options.csv_options.delimiter));
	}

	/* NULL STR */

	if (!duckdb_copy_options.csv_mode && duckdb_copy_options.csv_options.null_str != NULL) {
		elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY null available only in CSV mode");
		*options_valid = false;
		return options_string;
	}

	if (duckdb_copy_options.csv_options.null_str) {
		if (strchr(duckdb_copy_options.csv_options.null_str, '\r') != NULL ||
		    strchr(duckdb_copy_options.csv_options.null_str, '\n') != NULL) {
			elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY null representation cannot use newline or "
			              "carriage return");
			*options_valid = false;
			return options_string;
		}

		auto quoted_null_str = quote_literal_cstr(duckdb_copy_options.csv_options.null_str);
		options_parts.push_back(duckdb::StringUtil::Format("NULLSTR %s", quoted_null_str));
		pfree(quoted_null_str);
	}

	/* HEADER */

	if (!duckdb_copy_options.csv_mode && duckdb_copy_options.csv_options.include_header) {
		elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY header available only in CSV mode");
		*options_valid = false;
		return options_string;
	}

	if (duckdb_copy_options.csv_options.include_header) {
		options_parts.push_back(
		    duckdb::StringUtil::Format("HEADER %s", duckdb_copy_options.csv_options.include_header ? "true" : "false"));
	}

	/* QUOTE */

	if (!duckdb_copy_options.csv_mode && duckdb_copy_options.csv_options.quote != NULL) {
		elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY quote available only in CSV mode");
		*options_valid = false;
		return options_string;
	}

	if (duckdb_copy_options.csv_options.quote) {
		if (duckdb_copy_options.csv_mode && strlen(duckdb_copy_options.csv_options.quote) != 1) {
			elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY quote must be a single one-byte character");
			*options_valid = false;
			return options_string;
		}

		if (duckdb_copy_options.csv_mode && duckdb_copy_options.csv_options.delimiter &&
		    duckdb_copy_options.csv_options.delimiter[0] == duckdb_copy_options.csv_options.quote[0]) {
			elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY delimiter and quote must be different");
			*options_valid = false;
			return options_string;
		}

		options_parts.push_back(duckdb::StringUtil::Format("QUOTE \'%c\'", duckdb_copy_options.csv_options.quote));
	}

	/* ESCAPE  */

	if (!duckdb_copy_options.csv_mode && duckdb_copy_options.csv_options.escape != NULL) {
		elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY escape available only in CSV mode");
		*options_valid = false;
		return options_string;
	}

	if (duckdb_copy_options.csv_options.escape) {
		if (duckdb_copy_options.csv_mode && strlen(duckdb_copy_options.csv_options.escape) != 1) {
			elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY escape must be a single one-byte character");
			*options_valid = false;
			return options_string;
		}

		options_parts.push_back(duckdb::StringUtil::Format("ESCAPE \'%c\'", duckdb_copy_options.csv_options.escape));
	}

	/* FORCE_QUOTE*/

	if (!duckdb_copy_options.csv_mode && (duckdb_copy_options.csv_options.force_quote)) {
		elog(WARNING, "(PGDuckDB/CheckAndCreateCopyOptions) COPY force quote available only in CSV mode");
		*options_valid = false;
		return options_string;
	}

	if (duckdb_copy_options.csv_options.force_quote) {
		std::string force_quote_str = "FORCE_QUOTE (";
		bool first = true;
		foreach (lc, duckdb_copy_options.csv_options.force_quote) {
			if (!first) {
				force_quote_str += ", ";
			}
			first = false;
			String *quote = lfirst_node(String, lc);
			auto quoted_force_quote_val = quote_literal_cstr(quote->sval);
			force_quote_str += quoted_force_quote_val;
			pfree(quoted_force_quote_val);

		}
		force_quote_str += ")";
		options_parts.push_back(force_quote_str);
	}

	if (options_parts.size()) {

		options_string = "(";
		options_string +=
		    std::accumulate(std::begin(options_parts), std::end(options_parts), duckdb::string(),
		                    [](duckdb::string &acc, duckdb::string &s) { return acc.empty() ? s : acc + ", " + s; });
		options_string += ")";
	}

	options_string += ";";

	return options_string;
}

bool
DuckdbCopy(PlannedStmt *pstmt, const char *query_string, struct QueryEnvironment *query_env, uint64 *processed,
           bool *is_copy_to_cloud) {
	CopyStmt *copy_stmt = (CopyStmt *)pstmt->utilityStmt;

	*is_copy_to_cloud = false;

	if (!copy_stmt->filename) {
		return false;
	}

	/* Copy `filename` should start with S3/GS/R2 prefix */
	if (duckdb::string(copy_stmt->filename).rfind(s3_filename_prefix, 0) &&
	    duckdb::string(copy_stmt->filename).rfind(gcs_filename_prefix, 0) &&
	    duckdb::string(copy_stmt->filename).rfind(r2_filename_prefix, 0)) {
		return false;
	}

	*is_copy_to_cloud = true;

	/* We handle only COPY .. TO */
	if (copy_stmt->is_from) {
		return false;
	}

	bool options_valid = true;
	duckdb::string options_string = CheckAndCreateCopyOptions(copy_stmt, &options_valid);
	if (!options_valid) {
		return false;
	}

	auto filename_quoted = quote_literal_cstr(copy_stmt->filename);

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
		rewritten_query_string = duckdb::StringUtil::Format(
		    "COPY (%s) TO %s %s", pgduckdb_pg_get_querydef(query, false), filename_quoted, options_string);
	} else {
		bool copy_allowed = true;
		ParseState *pstate = make_parsestate(NULL);
		pstate->p_sourcetext = query_string;
		pstate->p_queryEnv = query_env;
		duckdb::string relation_copy_part = CreateRelationCopyString(pstate, copy_stmt, &copy_allowed);
		if (!copy_allowed) {
			return false;
		}
		rewritten_query_string =
		    duckdb::StringUtil::Format("COPY %s TO %s %s", relation_copy_part, filename_quoted, options_string);
	}

	elog(DEBUG2, "(PGDuckDB/CreateRelationCopyString) Rewritten query: \'%s\'", rewritten_query_string.c_str());
	pfree(filename_quoted);

	auto res = pgduckdb::DuckDBQueryOrThrow(rewritten_query_string);
	auto chunk = res->Fetch();
	*processed = chunk->GetValue(0, 0).GetValue<uint64_t>();
	return true;
}
