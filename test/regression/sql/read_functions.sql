-- PostgreSQL instance has data directory set to tmp_check/data so for all read functions argument
-- is relative to that data directory patch

-- read_parquet

SELECT count(r['sepal.length']) FROM read_parquet('../../data/iris.parquet') r;

SELECT r['sepal.length'] FROM read_parquet('../../data/iris.parquet') r ORDER BY r['sepal.length']  LIMIT 5;

SELECT r['sepal.length'], r['file_row_number'], r['filename']
    FROM read_parquet('../../data/iris.parquet', file_row_number => true, filename => true) r
    ORDER BY r['sepal.length']  LIMIT 5;

-- read_csv

SELECT count(r['sepal.length']) FROM read_csv('../../data/iris.csv') r;

SELECT r['sepal.length'] FROM read_csv('../../data/iris.csv') r ORDER BY r['sepal.length'] LIMIT 5;

SELECT r['sepal.length'], r['filename']
    FROM read_csv('../../data/iris.csv', filename => true, header => true) r
    ORDER BY r['sepal.length']  LIMIT 5;

SELECT * FROM read_csv('../../non-existing-file.csv');


-- delta_scan

SELECT duckdb.install_extension('delta');

SELECT count(r['a']) FROM delta_scan('../../data/delta_table') r;
SELECT * FROM delta_scan('../../data/delta_table') r WHERE (r['a'] = 1 OR r['b'] = 'delta_table_3');


-- iceberg_*

SELECT duckdb.install_extension('iceberg');

SELECT COUNT(r['l_orderkey']) FROM iceberg_scan('../../data/lineitem_iceberg', allow_moved_paths => true) r;

-- TPCH query #6
SELECT
	sum(r['l_extendedprice'] * r['l_discount']) as revenue
FROM
	iceberg_scan('../../data/lineitem_iceberg', allow_moved_paths => true) r
WHERE
	r['l_shipdate'] >= date '1997-01-01'
	AND r['l_shipdate'] < date '1997-01-01' + interval '1' year
	AND r['l_discount'] between 0.08 - 0.01 and 0.08 + 0.01
	AND r['l_quantity'] < 25
LIMIT 1;

SELECT * FROM iceberg_snapshots('../../data/lineitem_iceberg');
SELECT * FROM iceberg_metadata('../../data/lineitem_iceberg',  allow_moved_paths => true);

-- read_json

SELECT COUNT(r['a']) FROM read_json('../../data/table.json') r;
SELECT COUNT(r['a']) FROM read_json('../../data/table.json') r WHERE r['c'] > 50.4;
SELECT r['a'], r['b'], r['c'] FROM read_json('../../data/table.json') r WHERE r['c'] > 50.4 AND r['c'] < 51.2;
