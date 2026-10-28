\x on
SET bytea_output = 'escape';
SELECT * FROM duckdb.query($$
FROM test_all_types()
SELECT * exclude(
    small_enum,
    medium_enum,
    large_enum,
    struct,
    struct_of_arrays,
    array_of_structs,
    fixed_struct_array, -- ARRAY of STRUCT: depends on struct-in-array support (see struct_of_arrays/array_of_structs above), not nested-fixed-array #602
    struct_of_fixed_array, -- top-level STRUCT containing a fixed array: struct-of-array support, not #602
    fixed_nested_int_array, -- fixture has a NULL whole sub-array at an intermediate dimension ([[..],NULL,[..]]); PG multi-dim arrays can't hold a NULL sub-array, not #602
    fixed_nested_varchar_array, -- same intermediate-dimension NULL sub-array limitation as fixed_nested_int_array, not #602
    fixed_array_of_int_list, -- fixed ARRAY of variable-length LISTs ([[],[5 elems],[]]): jagged, not representable as a rectangular PG array, same class as nested_int_array
    nested_int_array, -- The nested array has different lengths, which is not possible in PG
    date, -- the min/max values of dates overflow in Postgres so selecting these would throw an error
    timestamp, -- the min/max values of timestamps overflow in Postgres so selecting these would throw an error
    timestamp_s, -- the min/max values of timestamps overflow in Postgres so selecting these would throw an error
    timestamp_ms, -- the min/max values of timestamps overflow in Postgres so selecting these would throw an error
    timestamp_ns, -- the min/max values of timestamps overflow in Postgres so selecting these would throw an error
    timestamp_tz, -- the min/max values of timestamps overflow in Postgres so selecting these would throw an error
    time_ns -- TIME_NS is not supported in Postgres
)
$$)
