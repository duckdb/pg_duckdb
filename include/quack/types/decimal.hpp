#pragma once

#define NUMERIC_POS						0x0000
#define NUMERIC_NEG						0x4000
#define NUMERIC_NAN						0xC000
#define NUMERIC_NULL						0xF000
#define NUMERIC_MAX_PRECISION			1000
#define NUMERIC_MAX_DISPLAY_SCALE		NUMERIC_MAX_PRECISION
#define NUMERIC_MIN_DISPLAY_SCALE		0
#define NUMERIC_MIN_SIG_DIGITS			16

#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	2	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	4

#define NUMERIC_EXT_FLAGBITS(n)	((n)->choice.n_header & NUMERIC_EXT_SIGN_MASK)
#define NUMERIC_IS_NAN(n)		((n)->choice.n_header == NUMERIC_NAN)
#define NUMERIC_IS_PINF(n)		((n)->choice.n_header == NUMERIC_PINF)
#define NUMERIC_IS_NINF(n)		((n)->choice.n_header == NUMERIC_NINF)
#define NUMERIC_IS_INF(n) \
	(((n)->choice.n_header & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF)

/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000
#define NUMERIC_SHORT		0x8000
#define NUMERIC_SPECIAL		0xC000

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_SHORT(n)		(NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_IS_SPECIAL(n)	(NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL)

#define NUMERIC_HDRSZ	(VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

/*
 * If the flag bits are NUMERIC_SHORT or NUMERIC_SPECIAL, we want the short
 * header; otherwise, we want the long one.  Instead of testing against each
 * value, we can just look at the high bit, for a slight efficiency gain.
 */
#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

/*
 * Definitions for special values (NaN, positive infinity, negative infinity).
 *
 * The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
 * infinity, 11 for negative infinity.  (This makes the sign bit match where
 * it is in a short-format value, though we make no use of that at present.)
 * We could mask off the remaining bits before testing the active bits, but
 * currently those bits must be zeroes, so masking would just add cycles.
 */
#define NUMERIC_EXT_SIGN_MASK	0xF000	/* high bits plus NaN/Inf flag bits */
#define NUMERIC_NAN				0xC000
#define NUMERIC_PINF			0xD000
#define NUMERIC_NINF			0xF000
#define NUMERIC_INF_SIGN_MASK	0x2000

/*
 * Short format definitions.
 */

#define NUMERIC_SHORT_SIGN_MASK			0x2000
#define NUMERIC_SHORT_DSCALE_MASK		0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT		7
#define NUMERIC_SHORT_DSCALE_MAX		\
	(NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK	0x0040
#define NUMERIC_SHORT_WEIGHT_MASK		0x003F
#define NUMERIC_SHORT_WEIGHT_MAX		NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN		(-(NUMERIC_SHORT_WEIGHT_MASK+1))

#define NUMERIC_DSCALE_MASK			0x3FFF
#define NUMERIC_DSCALE_MAX			NUMERIC_DSCALE_MASK

#define NUMERIC_SIGN(n) \
	(NUMERIC_IS_SHORT(n) ? \
		(((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? \
		 NUMERIC_NEG : NUMERIC_POS) : \
		(NUMERIC_IS_SPECIAL(n) ? \
		 NUMERIC_EXT_FLAGBITS(n) : NUMERIC_FLAGBITS(n)))
#define NUMERIC_DSCALE(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) \
		>> NUMERIC_SHORT_DSCALE_SHIFT \
	: ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	(((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
		~NUMERIC_SHORT_WEIGHT_MASK : 0) \
	 | ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) \
	: ((n)->choice.n_long.n_weight))

#define NUMERIC_DIGITS(num) (NUMERIC_HEADER_IS_SHORT(num) ? \
	(num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) \
	((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "utils/numeric.h"
}

typedef int16_t NumericDigit;

struct NumericShort
{
	uint16_t		n_header;		/* Sign + display scale + weight */
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

struct NumericLong
{
	uint16_t		n_sign_dscale;	/* Sign + display scale */
	int16_t		n_weight;		/* Weight of 1st digit	*/
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

union NumericChoice
{
	uint16_t		n_header;		/* Header word */
	struct NumericLong n_long;	/* Long form (4-byte header) */
	struct NumericShort n_short;	/* Short form (2-byte header) */
};

struct NumericData
{
	int32_t		vl_len_;		/* varlena header (do not touch directly!) */
	union NumericChoice choice; /* choice of format */
};

namespace quack {

struct NumericAsDouble : public duckdb::ExtraTypeInfo {
// Dummy struct to indicate at conversion that the source is a Numeric
public:
	NumericAsDouble() : ExtraTypeInfo(duckdb::ExtraTypeInfoType::INVALID_TYPE_INFO) {}
};

using duckdb::hugeint_t;

// Stolen from postgres, they hide these details in numeric.c
typedef struct NumericVar
{
    int32_t            ndigits;        /* # of digits in digits[] - can be 0! */
    int32_t            weight;            /* weight of first digit */
    int32_t            sign;            /* NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF */
    int32_t            dscale;            /* display scale */
    NumericDigit *buf;            /* start of palloc'd space for digits[] */
    NumericDigit *digits;        /* base-NBASE digits */
} NumericVar;


NumericVar FromNumeric(Numeric num)
{
	NumericVar dest;
	dest.ndigits = NUMERIC_NDIGITS(num);
	dest.weight = NUMERIC_WEIGHT(num);
	dest.sign = NUMERIC_SIGN(num);
	dest.dscale = NUMERIC_DSCALE(num);
	dest.digits = NUMERIC_DIGITS(num);
	dest.buf = NULL;			/* digits array is not palloc'd */
	return dest;
}

struct DecimalConversionInteger {
	static int64_t GetPowerOfTen(idx_t index) {
		static const int64_t POWERS_OF_TEN[] {1,
		                                      10,
		                                      100,
		                                      1000,
		                                      10000,
		                                      100000,
		                                      1000000,
		                                      10000000,
		                                      100000000,
		                                      1000000000,
		                                      10000000000,
		                                      100000000000,
		                                      1000000000000,
		                                      10000000000000,
		                                      100000000000000,
		                                      1000000000000000,
		                                      10000000000000000,
		                                      100000000000000000,
		                                      1000000000000000000};
		if (index >= 19) {
			throw duckdb::InternalException("DecimalConversionInteger::GetPowerOfTen - Out of range");
		}
		return POWERS_OF_TEN[index];
	}

	template <class T>
	static T Finalize(const NumericVar &numeric, T result) {
		return result;
	}
};

struct DecimalConversionHugeint {
	static hugeint_t GetPowerOfTen(idx_t index) {
		static const hugeint_t POWERS_OF_TEN[] {
		    hugeint_t(1),
		    hugeint_t(10),
		    hugeint_t(100),
		    hugeint_t(1000),
		    hugeint_t(10000),
		    hugeint_t(100000),
		    hugeint_t(1000000),
		    hugeint_t(10000000),
		    hugeint_t(100000000),
		    hugeint_t(1000000000),
		    hugeint_t(10000000000),
		    hugeint_t(100000000000),
		    hugeint_t(1000000000000),
		    hugeint_t(10000000000000),
		    hugeint_t(100000000000000),
		    hugeint_t(1000000000000000),
		    hugeint_t(10000000000000000),
		    hugeint_t(100000000000000000),
		    hugeint_t(1000000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10),
		    hugeint_t(1000000000000000000) * hugeint_t(100),
		    hugeint_t(1000000000000000000) * hugeint_t(1000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(10),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(100)};
		if (index >= 39) {
			throw duckdb::InternalException("DecimalConversionHugeint::GetPowerOfTen - Out of range");
		}
		return POWERS_OF_TEN[index];
	}

	static hugeint_t Finalize(const NumericVar &numeric, hugeint_t result) {
		return result;
	}
};

struct DecimalConversionDouble {
	static double GetPowerOfTen(idx_t index) {
		return pow(10, double(index));
	}

	static double Finalize(const NumericVar &numeric, double result) {
		return result / GetPowerOfTen(numeric.dscale);
	}
};

} // namespace quack
