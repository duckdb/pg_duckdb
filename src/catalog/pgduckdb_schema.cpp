#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
}

namespace pgduckdb {

PostgresSchema::PostgresSchema(Catalog &catalog, CreateSchemaInfo &info, Snapshot snapshot, PlannerInfo *planner_info)
    : SchemaCatalogEntry(catalog, info), snapshot(snapshot), catalog(catalog), planner_info(planner_info) {
}

void
PostgresSchema::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw duckdb::NotImplementedException("Scan(with context) not supported yet");
}

void
PostgresSchema::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw duckdb::NotImplementedException("Scan(no context) not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info, TableCatalogEntry &table) {
	throw duckdb::NotImplementedException("CreateIndex not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreateFunction not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw duckdb::NotImplementedException("CreateTable not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw duckdb::NotImplementedException("CreateView not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw duckdb::NotImplementedException("CreateSequence not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreateTableFunction not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreateCopyFunction not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) {
	throw duckdb::NotImplementedException("CreatePragmaFunction not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw duckdb::NotImplementedException("CreateCollation not supported yet");
}

optional_ptr<CatalogEntry>
PostgresSchema::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw duckdb::NotImplementedException("CreateType not supported yet");
}

static RelOptInfo *
FindMatchingRelEntry(Oid relid, PlannerInfo *planner_info) {
	int i = 1;
	RelOptInfo *node = nullptr;
	for (; i < planner_info->simple_rel_array_size; i++) {
		if (planner_info->simple_rte_array[i]->rtekind == RTE_SUBQUERY && planner_info->simple_rel_array[i]) {
			node = FindMatchingRelEntry(relid, planner_info->simple_rel_array[i]->subroot);
			if (node) {
				return node;
			}
		} else if (planner_info->simple_rte_array[i]->rtekind == RTE_RELATION) {
			if (relid == planner_info->simple_rte_array[i]->relid) {
				return planner_info->simple_rel_array[i];
			}
		};
	}
	return nullptr;
}

static bool IsIndexScan(const Path* nodePath) {
    if (nodePath == nullptr) {
        return false;
    }

    if (nodePath->pathtype == T_IndexScan || nodePath->pathtype == T_IndexOnlyScan) {
        return true;
    }

    return false;
}

optional_ptr<CatalogEntry>
PostgresSchema::GetEntry(CatalogTransaction transaction, CatalogType type, const string &entry_name) {
	if (type != CatalogType::TABLE_ENTRY) {
		throw duckdb::NotImplementedException("GetEntry (type: %s) not supported yet",
		                                      duckdb::EnumUtil::ToString(type));
	}

	auto it = tables.find(entry_name);
	if (it != tables.end()) {
		return it->second.get();
	}

	List *name_list = NIL;
	name_list = lappend(name_list, makeString(pstrdup(name.c_str())));
	name_list = lappend(name_list, makeString(pstrdup(entry_name.c_str())));

	RangeVar *table_range_var = makeRangeVarFromNameList(name_list);
	Oid rel_oid = RangeVarGetRelid(table_range_var, AccessShareLock, true);
	if (rel_oid == InvalidOid) {
		// Table could not be found
		return nullptr;
	}

	// Check if the Relation is a VIEW
	auto tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_oid));
	if (!HeapTupleIsValid(tuple)) {
		elog(ERROR, "Cache lookup failed for relation %u", rel_oid);
	}

	auto relForm = (Form_pg_class)GETSTRUCT(tuple);

	// Check if the relation is a view
	if (relForm->relkind == RELKIND_VIEW) {
		ReleaseSysCache(tuple);
		throw duckdb::NotImplementedException("Can't scan VIEWs yet");
	}
	ReleaseSysCache(tuple);

	Path *node_path = nullptr;

	if (planner_info) {
		auto node = FindMatchingRelEntry(rel_oid, planner_info);
		if (node) {
			node_path = get_cheapest_fractional_path(node, 0.0);
		}
	}

	if (IsIndexScan(node_path)) {
		throw duckdb::NotImplementedException("Can't perform INDEX SCAN yet");
	}

	CreateTableInfo info;
	info.table = name;
	if (!PostgresTable::PopulateColumns(info, rel_oid, snapshot)) {
		return nullptr;
	}
	tables[name] = duckdb::make_uniq<PostgresTable>(catalog, *this, info, rel_oid, snapshot);
	return tables[name].get();
}

void
PostgresSchema::DropEntry(ClientContext &context, DropInfo &info) {
	throw duckdb::NotImplementedException("DropEntry not supported yet");
}

void
PostgresSchema::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw duckdb::NotImplementedException("Alter not supported yet");
}

} // namespace pgduckdb
