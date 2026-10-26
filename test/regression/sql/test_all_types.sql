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
    fixed_nested_int_array,
    fixed_nested_varchar_array,
    fixed_struct_array,
    struct_of_fixed_array,
    fixed_array_of_int_list,
    list_of_fixed_int_array,
    nested_int_array, -- The nested array has different lengths, which is not possible in PG
    date, -- the min/max values of dates overflow in Postgres so selecting these would throw an error
    timestamp, -- the min/max values of timestamps overflow in Postgres so selecting these would throw an error
    timestamp_s, -- the min/max values of timestamps overflow in Postgres so selecting these would throw an error
    timestamp_ms, -- the min/max values of timestamps overflow in Postgres so selecting these would throw an error
    timestamp_ns, -- the min/max values of timestamps overflow in Postgres so selecting these would throw an error
    timestamp_tz, -- the min/max values of timestamps overflow in Postgres so selecting these would throw an error
    time_ns -- TIME_NS is not supported in Postgres
)
$$);

-- DuckDB types without a direct Postgres equivalent
SELECT * FROM duckdb.query($$ SELECT NULL AS null_value, [NULL] AS null_list $$);
SELECT * FROM duckdb.query($$ SELECT {'a': 1, 'b': [1, 2]}::VARIANT AS variant, ['x'::VARIANT, 2::VARIANT] AS variant_list $$);
SELECT r['v']['a'] AS a, r['v']['b'][1] AS b1 FROM duckdb.query($$ SELECT {'a': 1, 'b': [1, 2]}::VARIANT AS v $$) r;
