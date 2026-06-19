#pragma once

#include <cstdint>
#include <cstring>

extern "C" {
#include "postgres.h"
#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif
}

namespace pgduckdb {

Datum DetoastPostgresDatum(struct varlena *value, bool *should_free);

inline Datum
DetoastIfExternal(Datum value, bool *should_free) {
	auto *attr = reinterpret_cast<struct varlena *>(value);
	if (VARATT_IS_EXTERNAL(attr) || VARATT_IS_COMPRESSED(attr)) {
		return DetoastPostgresDatum(attr, should_free);
	}
	*should_free = false;
	return value;
}

static constexpr size_t kShortRealignBufSize = 144;

template <size_t BUF_SIZE>
inline Datum
DetoastPostgresDatumInline(Datum value, uint8_t (&stack_buf)[BUF_SIZE], bool *should_free) {
	static_assert(BUF_SIZE >= kShortRealignBufSize, "stack buffer too small");
	auto *attr = reinterpret_cast<struct varlena *>(value);
	if (VARATT_IS_EXTERNAL(attr) || VARATT_IS_COMPRESSED(attr)) {
		return DetoastPostgresDatum(attr, should_free);
	}
	*should_free = false;
	if (!VARATT_IS_SHORT(attr)) {
		return value;
	}
	const size_t data_size = VARSIZE_ANY_EXHDR(attr);
	if (data_size + VARHDRSZ > BUF_SIZE) {
		return DetoastPostgresDatum(attr, should_free);
	}
	SET_VARSIZE(stack_buf, data_size + VARHDRSZ);
	memcpy(stack_buf + VARHDRSZ, VARDATA_ANY(attr), data_size);
	return PointerGetDatum(stack_buf);
}

} // namespace pgduckdb
