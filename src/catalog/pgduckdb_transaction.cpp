#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"

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
#include "executor/nodeIndexscan.h"
#include "nodes/pathnodes.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/rel.h"
}

namespace duckdb {

PostgresTransaction::PostgresTransaction(TransactionManager &manager, ClientContext &context, PostgresCatalog &catalog,
                                         Snapshot snapshot)
    : Transaction(manager, context), catalog(catalog), snapshot(snapshot) {
}

PostgresTransaction::~PostgresTransaction() {
}

static RelOptInfo *
FindMatchingRelEntry(Oid relid, PlannerInfo *planner_info) {
	int i = 1;
	RelOptInfo *node = nullptr;
	for (; i < planner_info->simple_rel_array_size; i++) {
		if (planner_info->simple_rte_array[i]->rtekind == RTE_SUBQUERY && planner_info->simple_rel_array[i]
			&& planner_info->simple_rel_array[i]->subroot) {
			node = FindMatchingRelEntry(relid, planner_info->simple_rel_array[i]->subroot);
			//node = FindMatchingRelEntry(relid, planner_info->simple_rte_array[i]->subquery);
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

static bool
IsIndexScan(const Path *nodePath) {
	if (nodePath == nullptr) {
		return false;
	}

	if (nodePath->pathtype == T_IndexScan || nodePath->pathtype == T_IndexOnlyScan) {
		return true;
	}

	return false;
}

optional_ptr<CatalogEntry>
SchemaItems::GetTable(const string &entry_name, PlannerInfo *planner_info) {
	auto it = tables.find(entry_name);
	if (it != tables.end()) {
		return it->second.get();
	}
	auto snapshot = schema->snapshot;
	auto &catalog = schema->catalog;

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
		// Let the replacement scan handle this, the ReplacementScan replaces the view with its view_definition, which
		// will get bound again and hit a PostgresIndexTable / PostgresHeapTable.
		return nullptr;
	}
	ReleaseSysCache(tuple);

	Path *node_path = nullptr;
	RelOptInfo *node = nullptr;

	if (planner_info) {
		node = FindMatchingRelEntry(rel_oid, planner_info);
		ListCell *lc;
		/* We should prefer IndexOnlyScan */
		foreach (lc, node->pathlist) {
			Path *p = (Path *)lfirst(lc);
			if (p->pathtype == T_IndexOnlyScan) {
				node_path = p;
			}
		}
		if (node && (node_path == nullptr))
			node_path = get_cheapest_fractional_path(node, 0.0);
	}

	unique_ptr<PostgresTable> table;
	CreateTableInfo info;
	info.table = entry_name;
	Cardinality cardinality = node_path ? node_path->rows : 1;
	if (IsIndexScan(node_path)) {
		auto is_indexonly_scan = node_path->pathtype == T_IndexOnlyScan;
		if (!PostgresTable::PopulateIndexColumns(info, rel_oid, node_path, is_indexonly_scan)) {
			return nullptr;
		}
		table = make_uniq<PostgresIndexTable>(catalog, *schema, info, cardinality, snapshot, is_indexonly_scan,
		                                      node_path, planner_info, rel_oid);
	} else {
		if (!PostgresTable::PopulateHeapColumns(info, rel_oid, snapshot)) {
			return nullptr;
		}
		table = make_uniq<PostgresHeapTable>(catalog, *schema, info, cardinality, snapshot, rel_oid);
	}

	tables[entry_name] = std::move(table);
	return tables[entry_name].get();
}

optional_ptr<CatalogEntry>
PostgresTransaction::GetSchema(const string &name) {
	auto it = schemas.find(name);
	if (it != schemas.end()) {
		return it->second.schema.get();
	}

	CreateSchemaInfo create_schema;
	create_schema.schema = name;
	auto pg_schema = make_uniq<PostgresSchema>(catalog, create_schema, snapshot);
	schemas.emplace(std::make_pair(name, SchemaItems(std::move(pg_schema), name)));
	return schemas.at(name).schema.get();
}

optional_ptr<CatalogEntry>
PostgresTransaction::GetCatalogEntry(CatalogType type, const string &schema, const string &name) {
	auto scan_data = context.lock()->registered_state->Get<pgduckdb::PostgresContextState>("postgres_state");
	if (!scan_data) {
		throw InternalException("Could not find 'postgres_state' in 'PostgresTransaction::GetCatalogEntry'");
	}
	auto planner_info = scan_data->m_query_planner_info;
	switch (type) {
	case CatalogType::TABLE_ENTRY: {
		auto it = schemas.find(schema);
		if (it == schemas.end()) {
			return nullptr;
		}
		auto &schema_entry = it->second;
		return schema_entry.GetTable(name, planner_info);
	}
	case CatalogType::SCHEMA_ENTRY: {
		return GetSchema(schema);
	}
	default:
		return nullptr;
	}
}

} // namespace duckdb
