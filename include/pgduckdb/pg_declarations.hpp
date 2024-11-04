#pragma once

/*
This file contains a few Postgres declarations.

This is meant to be used in files that are mostly C++, but
need to interact with Postgres C code (eg. catalog implementation).

It should not include any C++ code, only Postgres C declarations.
*/

extern "C" {
struct SnapshotData;
typedef struct SnapshotData *Snapshot;

struct RelationData;
typedef struct RelationData *Relation;

typedef double Cardinality;

typedef unsigned int Oid;
}
