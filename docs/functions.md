# pg_duckdb Functions

By default, functions without a schema listed below are installed into `public`. You can choose to install these functions to an alternate location by running `CREATE EXTENSION pg_duckdb WITH SCHEMA schema`.

Note: `ALTER EXTENSION pg_duckdb WITH SCHEMA schema` is not currently supported.

## Data Lake Functions

| Name | Description |
| :--- | :---------- |
| [`read_parquet`](#read_parquet) | Read a parquet file |
| [`read_csv`](#read_csv) | Read a CSV file |
| [`read_json`](#read_json) | Read a JSON file |
| [`iceberg_scan`](#iceberg_scan) | Read an Iceberg dataset |
| [`iceberg_metadata`](#iceberg_metadata) | Read Iceberg metadata |
| [`iceberg_snapshots`](#iceberg_snapshots) | Read Iceberg snapshot information |
| [`delta_scan`](#delta_scan) | Read a Delta dataset |

## JSON Functions

All of the DuckDB [json functions and aggregates](https://duckdb.org/docs/data/json/json_functions.html). Postgres JSON/JSONB functions are not supported.

## Aggregates

|Name|Description|
| :--- | :---------- |
|[`approx_count_distinct`](https://duckdb.org/docs/sql/functions/aggregates.html#approximate-aggregates)|Gives the approximate count of distinct elements using HyperLogLog|

## Cache Management Functions

| Name | Description |
| :--- | :---------- |
| [`duckdb.cache`](#cache) | Caches a Parquet or CSV file to disk |
| [`duckdb.cache_info`](#cache_info) | Returns metadata about cached files |
| [`duckdb.cache_delete`](#cache_delete) | Deletes a file from the cache |

## DuckDB Administration Functions

| Name | Description |
| :--- | :---------- |
| [`duckdb.install_extension`](#install_extension) | Installs a DuckDB extension |
| [`duckdb.query`](#query) | Runs a SELECT query directly against DuckDB |
| [`duckdb.raw_query`](#raw_query) | Runs any query directly against DuckDB (meant for debugging)|
| [`duckdb.recycle_ddb`](#recycle_ddb) | Force a reset the DuckDB instance in the current connection (meant for debugging) |

## Motherduck Functions

| Name | Description |
| :--- | :---------- |
| [`duckdb.force_motherduck_sync`](#force_motherduck_sync) | Forces a full resync of Motherduck databases and schemas to Postgres (meant for debugging) |

## Detailed Descriptions

#### <a name="read_parquet"></a>`read_parquet(path TEXT or TEXT[], /* optional parameters */) -> SETOF duckdb.row`

Reads a parquet file, either from a remote location (via httpfs) or a local file.

This returns DuckDB rows, you can expand them using `*` or you can select specific columns using the `r['mycol']` syntax. If you want to select specific columns you should give the function call an easy alias, like `r`. For example:

```sql
SELECT * FROM read_parquet('file.parquet');
SELECT r['id'], r['name'] FROM read_parquet('file.parquet') r WHERE r['age'] > 21;
SELECT COUNT(*) FROM read_parquet('file.parquet');
```

Further information:

* [DuckDB Parquet documentation](https://duckdb.org/docs/data/parquet/overview)
* [DuckDB httpfs documentation](https://duckdb.org/docs/extensions/httpfs/https.html)

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| path | text or text[] | The path, either to a remote httpfs file or a local file (if enabled), of the parquet file(s) to read. The path can be a glob or array of files to read. |

##### Optional Parameters

Optional parameters mirror [DuckDB's read_parquet function](https://duckdb.org/docs/data/parquet/overview.html#parameters). To specify optional parameters, use `parameter := 'value'`.

#### <a name="read_csv"></a>`read_csv(path TEXT or TEXT[], /* optional parameters */) -> SETOF duckdb.row`

Reads a CSV file, either from a remote location (via httpfs) or a local file.

This returns DuckDB rows, you can expand them using `*` or you can select specific columns using the `r['mycol']` syntax. If you want to select specific columns you should give the function call an easy alias, like `r`. For example:

```sql
SELECT * FROM read_csv('file.csv');
SELECT r['id'], r['name'] FROM read_csv('file.csv') r WHERE r['age'] > 21;
SELECT COUNT(*) FROM read_csv('file.csv');
```

Further information:

* [DuckDB CSV documentation](https://duckdb.org/docs/data/csv/overview)
* [DuckDB httpfs documentation](https://duckdb.org/docs/extensions/httpfs/https.html)

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| path | text or text[] | The path, either to a remote httpfs file or a local file (if enabled), of the CSV file(s) to read. The path can be a glob or array of files to read. |

##### Optional Parameters

Optional parameters mirror [DuckDB's read_csv function](https://duckdb.org/docs/data/csv/overview.html#parameters). To specify optional parameters, use `parameter := 'value'`.

Compatibility notes:

* `columns` is not currently supported.
* `nullstr` must be an array (`TEXT[]`).

#### <a name="read_json"></a>`read_json(path TEXT or TEXT[], /* optional parameters */) -> SETOF duckdb.row`

Reads a JSON file, either from a remote location (via httpfs) or a local file.

This returns DuckDB rows, you can expand them using `*` or you can select specific columns using the `r['mycol']` syntax. If you want to select specific columns you should give the function call an easy alias, like `r`. For example:

```sql
SELECT * FROM read_parquet('file.parquet');
SELECT r['id'], r['name'] FROM read_parquet('file.parquet') r WHERE r['age'] > 21;
SELECT COUNT(*) FROM read_parquet('file.parquet');
```

Further information:

* [DuckDB JSON documentation](https://duckdb.org/docs/data/json/overview)

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| path | text or text[] | The path, either to a remote httpfs file or a local file (if enabled), of the JSON file(s) to read. The path can be a glob or array of files to read. |

##### Optional Parameters

Optional parameters mirror [DuckDB's read_json function](https://duckdb.org/docs/data/json/loading_json#json-read-functions). To specify optional parameters, use `parameter := 'value'`.

Compatibility notes:

* `columns` is not currently supported.

#### <a name="iceberg_scan"></a>`iceberg_scan(path TEXT, /* optional parameters */) -> SETOF duckdb.row`

Reads an Iceberg table, either from a remote location (via httpfs) or a local directory.

To use `iceberg_scan`, you must enable the `iceberg` extension:

```sql
SELECT duckdb.install_extension('iceberg');
```

This returns DuckDB rows, you can expand them using `*` or you can select specific columns using the `r['mycol']` syntax. If you want to select specific columns you should give the function call an easy alias, like `r`. For example:

```sql
SELECT * FROM iceberg_scan('data/iceberg/table');
SELECT r['id'], r['name'] FROM iceberg_scan('data/iceberg/table') r WHERE r['age'] > 21;
SELECT COUNT(*) FROM iceberg_scan('data/iceberg/table');
```

Further information:

* [DuckDB Iceberg extension documentation](https://duckdb.org/docs/extensions/iceberg.html)

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| path | text | The path, either to a remote httpfs location or a local location (if enabled), of the Iceberg table to read. |

##### Optional Arguments

Optional parameters mirror DuckDB's `iceberg_scan` function based on the DuckDB source code. However, documentation on these parameters is limited. To specify optional parameters, use `parameter := 'value'`.

| Name | Type | Default | Description |
| :--- | :--- | :------ | :---------- |
| allowed_moved_paths | boolean | false | Ensures that some path resolution is performed, which allows scanning Iceberg tables that are moved. |
| mode | text | `''` | |
| metadata_compression_codec | text | `'none'` | |
| skip_schema_inference | boolean | false | |
| version | text | `'version-hint.text'` | |
| version_name_format | text | `'v%s%s.metadata.json,%s%s.metadata.json'` | |

#### <a name="iceberg_metadata"></a>`iceberg_metadata(path TEXT, /* optional parameters */) -> SETOF iceberg_metadata_record`

To use `iceberg_metadata`, you must enable the `iceberg` extension:

```sql
SELECT duckdb.install_extension('iceberg');
```

Return metadata about an iceberg table. Data is returned as a set of `icerberg_metadata_record`, which is defined as:

```sql
CREATE TYPE duckdb.iceberg_metadata_record AS (
  manifest_path TEXT,
  manifest_sequence_number NUMERIC,
  manifest_content TEXT,
  status TEXT,
  content TEXT,
  file_path TEXT
);
```

Further information:

* [DuckDB Iceberg extension documentation](https://duckdb.org/docs/extensions/iceberg.html)

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| path | text | The path, either to a remote httpfs location or a local location (if enabled), of the Iceberg table to read. |

##### Optional Arguments

Optional parameters mirror DuckDB's `iceberg_metadata` function based on the DuckDB source code. However, documentation on these parameters is limited. To specify optional parameters, use `parameter := 'value'`.

| Name | Type | Default | Description |
| :--- | :--- | :------ | :---------- |
| allowed_moved_paths | boolean | false | Ensures that some path resolution is performed, which allows scanning Iceberg tables that are moved. |
| metadata_compression_codec | text | `'none'` | |
| skip_schema_inference | boolean | false | |
| version | text | `'version-hint.text'` | |
| version_name_format | text | `'v%s%s.metadata.json,%s%s.metadata.json'` | |

#### <a name="iceberg_snapshots"></a>`iceberg_snapshots(path TEXT, /* optional parameters */) -> TODO`

TODO

#### <a name="delta_scan"></a>`delta_scan(path TEXT) -> SETOF duckdb.row`

Reads a delta dataset, either from a remote (via httpfs) or a local location.

To use `delta_scan`, you must enable the `delta` extension:

```sql
SELECT duckdb.install_extension('delta');
```

This returns DuckDB rows, you can expand them using `*` or you can select specific columns using the `r['mycol']` syntax. If you want to select specific columns you should give the function call an easy alias, like `r`. For example:

```sql
SELECT * FROM delta_scan('/path/to/delta/dataset');
SELECT r['id'], r['name'] FROM delta_scan('/path/to/delta/dataset') r WHERE r['age'] > 21;
SELECT COUNT(*) FROM delta_scan('/path/to/delta/dataset');
```


Further information:

* [DuckDB Delta extension documentation](https://duckdb.org/docs/extensions/delta)

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| path | text | The path, either to a remote httpfs location or a local location (if enabled) of the delta dataset to read. |

#### <a name="cache"></a>`duckdb.cache(path TEXT, type TEXT) -> bool`

Caches a parquet or CSV file to local disk. The file is downloaded synchronously during the execution of the function. Once cached, the cached file is used automatically whenever that URL is used in other httpfs calls, provided that the remote data has not changed. Data is stored based on the eTag of the remote file.

Note that cache management is not automated. Cached data must be deleted manually.

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| path | text | The path to a remote httpfs location to cache. |
| type | text | File type, either `parquet` or `csv` |

#### <a name="cache_info"></a>`duckdb.cache_info() -> (remote_path text, cache_key text, cache_file_size BIGINT, cache_file_timestamp TIMESTAMPTZ)`

Inspects which remote files are currently cached in DuckDB. The returned data is as follows:

| Name | Type | Description |
| :--- | :--- | :---------- |
| remote_path | text | The original path to the remote httpfs location that was cached |
| cache_key | text | The cache key (eTag) used to store the file |
| cache_file_size | bigint | File size in bytes |
| cache_file_timestamp | timestamptz | Creation time of the cached file |

#### <a name="cache_delete"></a>`duckdb.cache_delete(cache_key text) -> bool`

Deletes a file from the DuckDB cache using the `unique cache_key` of the file.

Example: To delete any copies of a particular remote file:

```sql
SELECT duckdb.cache_delete(cache_key)
FROM duckdb.cache_info()
WHERE remote_path = '...';
```

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| cache_key | text | The cache key (eTag) to delete |

#### <a name="install_extension"></a>`duckdb.install_extension(extension_name TEXT) -> bool`

Installs a DuckDB extension and configures it to be loaded automatically in
every session that uses pg_duckdb.

```sql
SELECT duckdb.install_extension('iceberg');
```

##### Security

Since this function can be used to install and download any of the official
extensions it can only be executed by a superuser by default. To allow
execution by some other admin user, such as `my_admin`, you can grant such a
user the following permissions:

```sql
GRANT ALL ON FUNCTION duckdb.install_extension(TEXT) TO my_admin;
GRANT ALL ON TABLE duckdb.extensions TO my_admin;
GRANT ALL ON SEQUENCE duckdb.extensions_table_seq TO my_admin;
```

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| extension_name | text | The name of the extension to install |

#### <a name="query"></a>`duckdb.query(query TEXT) -> SETOF duckdb.row`

Executes the given SELECT query directly against DuckDB. This can be useful if DuckDB syntax makes the query easier to write or if you want to use a function that is not exposed by pg_duckdb yet. If you use it because of a missing function in pg_duckdb, please also open an issue on the GitHub repository so that we can add support. For example the below query shows a query that puts `FROM` before `SELECT` and uses a list comprehension. Both of those features are not supported in Postgres.

```sql
SELECT * FROM duckdb.query('FROM range(10) as a(a) SELECT [a for i in generate_series(0, a)] as arr');
```

#### <a name="raw_query"></a>`duckdb.raw_query(extension_name TEXT) -> void`

Runs an arbitrary query directly against DuckDB. Compared to `duckdb.query`, this function can execute any query, not just SELECT queries. The main downside is that it doesn't return its result as rows, but instead sends the query result to the logs. So the recommendation is to use `duckdb.query` when possible, but if you need to run e.g. some DDL you can use this function.

#### <a name="recycle_ddb"></a>`duckdb.recycle_ddb() -> void`

pg_duckdb keeps the DuckDB instance open inbetween transactions. This is done
to save session level state, such as manually done `SET` commands. If you want
to clear this session level state for some reason you can close the currently
open DuckDB instance using:

```sql
CALL duckdb.recycle_ddb();
```

#### <a name="force_motherduck_sync"></a>`duckdb.force_motherduck_sync(drop_with_cascade BOOLEAN DEFAULT false)`

WARNING: There are known issues with this function currently. For now you
should use the following command to retrigger a sync:

```
select * from pg_terminate_backend((select pid from pg_stat_activity where backend_type = 'pg_duckdb sync worker'));
```

`pg_duckdb` will normally automatically synchronize your MotherDuck tables with Postgres using a Postgres background worker. Sometimes this synchronization fails. This can happen for various reasons, but often this is due to permission issues or users having created dependencies on MotherDuck tables that need to be updated. In those cases this function can be helpful for a few reasons:

1. To show the ERRORs that happen during syncing
2. To retrigger a sync after fixing the issue
3. To drop the MotherDuck tables with `CASCADE` to drop all objects that depend on it.

For the first two usages you can simply call this procedure like follows:

```sql
CALL duckdb.force_motherduck_sync();
```

But for the third usage you need to run pass it the `drop_with_cascade` parameter:

```sql
CALL duckdb.force_motherduck_sync(drop_with_cascade := true);
```

NOTE: Dropping with cascade will drop all objects that depend on the MotherDuck tables. This includes all views, functions, and tables that depend on the MotherDuck tables. This can be a destructive operation, so use with caution.
