#include "pgduckdb/pgduckdb_detoast.hpp"

extern "C" {
#include "fmgr.h"
#include "utils/varlena.h"
}

#include <cstring>

extern "C" {
void *duckdb_malloc(size_t size);
}

namespace pgduckdb {

Datum
DetoastPostgresDatum(struct varlena *attr, bool *should_free) {
	if (!VARATT_IS_EXTENDED(attr)) {
		*should_free = false;
		return PointerGetDatum(attr);
	}

	struct varlena *detoasted = (struct varlena *)PG_DETOAST_DATUM(attr);
	Size size = VARSIZE_ANY(detoasted);
	struct varlena *result = (struct varlena *)duckdb_malloc(size);
	memcpy(result, detoasted, size);

	if (detoasted != attr) {
		pfree(detoasted);
	}

	*should_free = true;
	return PointerGetDatum(result);
}

} // namespace pgduckdb
