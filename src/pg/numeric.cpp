#include "pgduckdb/pg/numeric.hpp"

#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "utils/numeric.h"
#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif
}

#define NUMERIC_NAN               0xC000
#define NUMERIC_NULL              0xF000
#define NUMERIC_MAX_PRECISION     1000
#define NUMERIC_MAX_DISPLAY_SCALE NUMERIC_MAX_PRECISION
#define NUMERIC_MIN_DISPLAY_SCALE 0
#define NUMERIC_MIN_SIG_DIGITS    16

#define HALF_NBASE 5000

#define MUL_GUARD_DIGITS 2 /* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS 4

#define NUMERIC_EXT_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_EXT_SIGN_MASK)
#define NUMERIC_IS_NAN(n)       ((n)->choice.n_header == NUMERIC_NAN)
#define NUMERIC_IS_PINF(n)      ((n)->choice.n_header == NUMERIC_PINF)
#define NUMERIC_IS_NINF(n)      ((n)->choice.n_header == NUMERIC_NINF)
#define NUMERIC_IS_INF(n)       (((n)->choice.n_header & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF)

/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_SHORT     0x8000
#define NUMERIC_SPECIAL   0xC000

#define NUMERIC_FLAGBITS(n)   ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_SHORT(n)   (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_IS_SPECIAL(n) (NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL)

#define NUMERIC_HDRSZ       (VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

/*
 * If the flag bits are NUMERIC_SHORT or NUMERIC_SPECIAL, we want the short
 * header; otherwise, we want the long one.  Instead of testing against each
 * value, we can just look at the high bit, for a slight efficiency gain.
 */
#define NUMERIC_HEADER_IS_SHORT(n) (((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n)     (VARHDRSZ + sizeof(uint16) + (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

/*
 * Definitions for special values (NaN, positive infinity, negative infinity).
 *
 * The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
 * infinity, 11 for negative infinity.  (This makes the sign bit match where
 * it is in a short-format value, though we make no use of that at present.)
 * We could mask off the remaining bits before testing the active bits, but
 * currently those bits must be zeroes, so masking would just add cycles.
 */
#define NUMERIC_EXT_SIGN_MASK 0xF000 /* high bits plus NaN/Inf flag bits */
#define NUMERIC_NAN           0xC000
#define NUMERIC_PINF          0xD000
#define NUMERIC_NINF          0xF000
#define NUMERIC_INF_SIGN_MASK 0x2000

/*
 * Short format definitions.
 */

#define NUMERIC_SHORT_SIGN_MASK        0x2000
#define NUMERIC_SHORT_DSCALE_MASK      0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT     7
#define NUMERIC_SHORT_DSCALE_MAX       (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK      0x003F
#define NUMERIC_SHORT_WEIGHT_MAX       NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN       (-(NUMERIC_SHORT_WEIGHT_MASK + 1))

#define NUMERIC_DSCALE_MASK 0x3FFF
#define NUMERIC_DSCALE_MAX  NUMERIC_DSCALE_MASK

#define NUMERIC_SIGN(n)                                                                                                \
	(NUMERIC_IS_SHORT(n) ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? NUMERIC_NEG : NUMERIC_POS)      \
	                     : (NUMERIC_IS_SPECIAL(n) ? NUMERIC_EXT_FLAGBITS(n) : NUMERIC_FLAGBITS(n)))
#define NUMERIC_DSCALE(n)                                                                                              \
	(NUMERIC_HEADER_IS_SHORT((n))                                                                                      \
	     ? ((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT                    \
	     : ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(n)                                                                                              \
	(NUMERIC_HEADER_IS_SHORT((n))                                                                                      \
	     ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) |         \
	        ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK))                                                \
	     : ((n)->choice.n_long.n_weight))

#define NUMERIC_DIGITS(num)  (NUMERIC_HEADER_IS_SHORT(num) ? (num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) ((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))
#define NUMERIC_CAN_BE_SHORT(scale, weight)                                                                            \
	((scale) <= NUMERIC_SHORT_DSCALE_MAX && (weight) <= NUMERIC_SHORT_WEIGHT_MAX &&                                    \
	 (weight) >= NUMERIC_SHORT_WEIGHT_MIN)

Numeric
PGCreateNumeric(const NumericVar &var, bool *have_error) {
	Numeric result;
	NumericDigit *digits = var.digits;
	int weight = var.weight;
	int sign = var.sign;
	int n;
	Size len;

	if (have_error)
		*have_error = false;

	if ((sign & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL) {
		/*
		 * Verify valid special value.  This could be just an Assert, perhaps,
		 * but it seems worthwhile to expend a few cycles to ensure that we
		 * never write any nonzero reserved bits to disk.
		 */
		if (!(sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF)) {
			elog(WARNING, "(PGDuckdDB/CreateNumeric) Invalid numeric sign value 0x%x", sign);
			*have_error = true;
			return NULL;
		}

		result = (Numeric)palloc(NUMERIC_HDRSZ_SHORT);

		SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
		result->choice.n_header = sign;
		/* the header word is all we need */
		return result;
	}

	n = var.ndigits;

	/* truncate leading zeroes */
	while (n > 0 && *digits == 0) {
		digits++;
		weight--;
		n--;
	}
	/* truncate trailing zeroes */
	while (n > 0 && digits[n - 1] == 0)
		n--;

	/* If zero result, force to weight=0 and positive sign */
	if (n == 0) {
		weight = 0;
		sign = NUMERIC_POS;
	}

	/* Build the result */
	if (NUMERIC_CAN_BE_SHORT(var.dscale, weight)) {
		len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
		result = (Numeric)palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_short.n_header =
		    (sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
		    (var.dscale << NUMERIC_SHORT_DSCALE_SHIFT) | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
		    (weight & NUMERIC_SHORT_WEIGHT_MASK);
	} else {
		len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
		result = (Numeric)palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_long.n_sign_dscale = sign | (var.dscale & NUMERIC_DSCALE_MASK);
		result->choice.n_long.n_weight = weight;
	}

	Assert(NUMERIC_NDIGITS(result) == n);
	if (n > 0)
		memcpy(NUMERIC_DIGITS(result), digits, n * sizeof(NumericDigit));

	/* Check for overflow of int16 fields */
	if (NUMERIC_WEIGHT(result) != weight || NUMERIC_DSCALE(result) != var.dscale) {
		if (have_error) {
			*have_error = true;
			return NULL;
		} else {
			ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value overflows numeric format")));
		}
	}
	return result;
}

Numeric
CreateNumeric(const NumericVar &var, bool *have_error) {
	return PostgresFunctionGuard(PGCreateNumeric, var, have_error);
}

NumericVar
FromNumeric(Numeric num) {
	NumericVar dest;
	dest.ndigits = NUMERIC_NDIGITS(num);
	dest.weight = NUMERIC_WEIGHT(num);
	dest.sign = NUMERIC_SIGN(num);
	dest.dscale = NUMERIC_DSCALE(num);
	dest.digits = NUMERIC_DIGITS(num);
	dest.buf = NULL; /* digits array is not palloc'd */
	return dest;
}
