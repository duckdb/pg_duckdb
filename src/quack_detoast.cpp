#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "varatt.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

#include "access/detoast.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/toast_internals.h"
#include "common/pg_lzcompress.h"
#include "utils/expandeddatum.h"
}

#include "quack/quack_types.hpp"
#include "quack/quack_detoast.hpp"

/*
 * Following functions are direct logic found in postgres code but for duckdb execution they are needed to be thread
 * safe. Functions as palloc/pfree are exchanged with duckdb_malloc/duckdb_free. Access to toast table is protected with
 * lock also for thread safe reasons. This is initial implementation but should be revisisted in future for better
 * performances.
 */

namespace quack {

struct varlena *
_pglz_decompress_datum(const struct varlena *value) {
	struct varlena *result;
	int32 rawsize;

	result = (struct varlena *)duckdb_malloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);

	rawsize = pglz_decompress((char *)value + VARHDRSZ_COMPRESSED, VARSIZE(value) - VARHDRSZ_COMPRESSED,
	                          VARDATA(result), VARDATA_COMPRESSED_GET_EXTSIZE(value), true);
	if (rawsize < 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

struct varlena *
_lz4_decompress_datum(const struct varlena *value) {
#ifndef USE_LZ4
	NO_LZ4_SUPPORT();
	return NULL; /* keep compiler quiet */
#else
	int32 rawsize;
	struct varlena *result;

	result = (struct varlena *)duckdb_malloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);

	rawsize = LZ4_decompress_safe((char *)value + VARHDRSZ_COMPRESSED, VARDATA(result),
	                              VARSIZE(value) - VARHDRSZ_COMPRESSED, VARDATA_COMPRESSED_GET_EXTSIZE(value));
	if (rawsize < 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg_internal("compressed lz4 data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

static struct varlena *
_toast_decompress_datum(struct varlena *attr) {
	switch (TOAST_COMPRESS_METHOD(attr)) {
	case TOAST_PGLZ_COMPRESSION_ID:
		return _pglz_decompress_datum(attr);
	case TOAST_LZ4_COMPRESSION_ID:
		return _lz4_decompress_datum(attr);
	default:
		elog(ERROR, "invalid compression method id %d", TOAST_COMPRESS_METHOD(attr));
		return NULL; /* keep compiler quiet */
	}
}

static struct varlena *
_toast_fetch_datum(struct varlena *attr, std::mutex &lock) {
	Relation toastrel;
	struct varlena *result;
	struct varatt_external toast_pointer;
	int32 attrsize;

	if (!VARATT_IS_EXTERNAL_ONDISK(attr))
		elog(ERROR, "toast_fetch_datum shouldn't be called for non-ondisk datums");

	/* Must copy to access aligned fields */
	VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

	attrsize = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

	result = (struct varlena *)duckdb_malloc(attrsize + VARHDRSZ);

	if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)) {
		SET_VARSIZE_COMPRESSED(result, attrsize + VARHDRSZ);
	} else {
		SET_VARSIZE(result, attrsize + VARHDRSZ);
	}

	if (attrsize == 0)
		return result;

	lock.lock();
	toastrel = table_open(toast_pointer.va_toastrelid, AccessShareLock);
	table_relation_fetch_toast_slice(toastrel, toast_pointer.va_valueid, attrsize, 0, attrsize, result);
	table_close(toastrel, AccessShareLock);
	lock.unlock();

	return result;
}

Datum
DetoastPostgresDatum(struct varlena *attr, std::mutex &lock, bool *shouldFree) {
	struct varlena *toastedValue = nullptr;
	*shouldFree = true;
	if (VARATT_IS_EXTERNAL_ONDISK(attr)) {
		toastedValue = _toast_fetch_datum(attr, lock);
		if (VARATT_IS_COMPRESSED(toastedValue)) {
			struct varlena *tmp = toastedValue;
			toastedValue = _toast_decompress_datum(tmp);
			duckdb_free(tmp);
		}
	} else if (VARATT_IS_EXTERNAL_INDIRECT(attr)) {
		struct varatt_indirect redirect;
		VARATT_EXTERNAL_GET_POINTER(redirect, attr);
		toastedValue = (struct varlena *)redirect.pointer;
		toastedValue = reinterpret_cast<struct varlena *>(DetoastPostgresDatum(attr, lock, shouldFree));
		if (attr == (struct varlena *)redirect.pointer) {
			struct varlena *result;
			result = (struct varlena *)(VARSIZE_ANY(attr));
			memcpy(result, attr, VARSIZE_ANY(attr));
			toastedValue = result;
		}
	} else if (VARATT_IS_EXTERNAL_EXPANDED(attr)) {
		ExpandedObjectHeader *eoh;
		Size resultsize;
		eoh = DatumGetEOHP(PointerGetDatum(attr));
		resultsize = EOH_get_flat_size(eoh);
		toastedValue = (struct varlena *)duckdb_malloc(resultsize);
		EOH_flatten_into(eoh, (void *)toastedValue, resultsize);
	} else if (VARATT_IS_COMPRESSED(attr)) {
		toastedValue = _toast_decompress_datum(attr);
	} else if (VARATT_IS_SHORT(attr)) {
		Size data_size = VARSIZE_SHORT(attr) - VARHDRSZ_SHORT;
		Size new_size = data_size + VARHDRSZ;
		toastedValue = (struct varlena *)duckdb_malloc(new_size);
		SET_VARSIZE(toastedValue, new_size);
		memcpy(VARDATA(toastedValue), VARDATA_SHORT(attr), data_size);
	} else {
		toastedValue = attr;
		*shouldFree = false;
	}

	return reinterpret_cast<Datum>(toastedValue);
}

} // namespace quack