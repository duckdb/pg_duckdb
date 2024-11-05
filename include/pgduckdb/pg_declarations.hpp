#pragma once

/*
This file contains a few Postgres declarations.

This is meant to be used in files that are mostly C++, but
need to interact with Postgres C code (eg. catalog implementation).

It should not include any C++ code, only Postgres C declarations.
*/

extern "C" {
typedef double Cardinality;

struct FormData_pg_class;
typedef FormData_pg_class *Form_pg_class;

struct Node;

typedef unsigned int Oid;

struct RelationData;
typedef struct RelationData *Relation;

struct SnapshotData;
typedef struct SnapshotData *Snapshot;

}
