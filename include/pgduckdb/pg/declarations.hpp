#pragma once

#include <inttypes.h>

/*
This file contains a few Postgres declarations.

This is meant to be used in files that are mostly C++, but
need to interact with Postgres C code (eg. catalog implementation).

It should not include any C++ code, only Postgres C declarations.
*/

extern "C" {
typedef uint32_t BlockNumber;

typedef double Cardinality;

typedef uintptr_t Datum;

struct FormData_pg_attribute;
typedef FormData_pg_attribute *Form_pg_attribute;

struct FormData_pg_class;
typedef FormData_pg_class *Form_pg_class;

struct HeapTupleData;

struct Node;

typedef unsigned int Oid;

struct RelationData;
typedef struct RelationData *Relation;

struct SnapshotData;
typedef struct SnapshotData *Snapshot;

struct TupleDescData;
typedef struct TupleDescData *TupleDesc;

struct TupleTableSlot;

}
