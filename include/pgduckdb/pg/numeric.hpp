#pragma once

#include <cstdint>

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NBASE       10000

// From PG's src/backend/utils/adt/numeric.c
// Warning - different from PG's "public" NumericDigit which is an unsigned char
typedef int16_t NumericDigit;

struct NumericShort {
	uint16_t n_header;     /* Sign + display scale + weight */
	NumericDigit n_data[]; /* Digits */
};

struct NumericLong {
	uint16_t n_sign_dscale; /* Sign + display scale */
	int16_t n_weight;       /* Weight of 1st digit	*/
	NumericDigit n_data[];  /* Digits */
};

union NumericChoice {
	uint16_t n_header;           /* Header word */
	struct NumericLong n_long;   /* Long form (4-byte header) */
	struct NumericShort n_short; /* Short form (2-byte header) */
};

struct NumericData {
	int32_t vl_len_;            /* varlena header (do not touch directly!) */
	union NumericChoice choice; /* choice of format */
};

typedef struct NumericData *Numeric;

// Stolen from postgres, they hide these details in numeric.c
typedef struct NumericVar {
	int32_t ndigits;      /* # of digits in digits[] - can be 0! */
	int32_t weight;       /* weight of first digit */
	int32_t sign;         /* NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF */
	int32_t dscale;       /* display scale */
	NumericDigit *buf;    /* start of palloc'd space for digits[] */
	NumericDigit *digits; /* base-NBASE digits */
} NumericVar;

/*
 *	Create the packed db numeric format in palloc()'d memory from
 *	a variable.  This will handle NaN and Infinity cases.
 *
 *	If "have_error" isn't NULL, on overflow *have_error is set to true and
 *	NULL is returned.  This is helpful when caller needs to handle errors.
 */
Numeric CreateNumeric(const NumericVar &var, bool *have_error);

NumericVar FromNumeric(Numeric num);
