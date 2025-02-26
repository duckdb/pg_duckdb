\x on
SET bytea_output = 'escape';
SELECT * FROM duckdb.query($$
FROM test_all_types()
SELECT * exclude(
    tinyint, -- PG14 outputs this differently currently
    varint,
    TIME,
    time_tz,
    small_enum,
    medium_enum,
    large_enum,
    timestamptz_array,
    struct,
    struct_of_arrays,
    array_of_structs,
    map,
    "union",
    fixed_int_array,
    fixed_varchar_array,
    fixed_nested_int_array,
    fixed_nested_varchar_array,
    fixed_struct_array,
    struct_of_fixed_array,
    fixed_array_of_int_list,
    list_of_fixed_int_array,
    nested_int_array, -- The nested array has different lengths, which is not possible in PG
)
$$)
