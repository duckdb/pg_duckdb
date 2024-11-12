#pragma once

#include "postgres.h"
#include "access/heapam.h"

bool HeapTupleSatisfiesVisibilityNoHintBits(HeapTuple htup, Snapshot snapshot, Buffer buffer);
