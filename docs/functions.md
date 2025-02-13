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
| [`duckdb.raw_query`](#raw_query) | Runs a query directly against DuckDB (meant for debugging)|
| [`duckdb.recycle_ddb`](#recycle_ddb) | Force a reset the DuckDB instance in the current connection (meant for debugging) |

## Motherduck Functions

| Name | Description |
| :--- | :---------- |
| [`duckdb.force_motherduck_sync`](#force_motherduck_sync) | Forces a full resync of Motherduck databases and schemas to Postgres (meant for debugging) |

## Detailed Descriptions

#### <a name="read_parquet"></a>`read_parquet(path TEXT or TEXT[], /* optional parameters */) -> SETOF record`

Reads a parquet file, either from a remote location (via httpfs) or a local file.

Returns a record set (`SETOF record`). Functions that return record sets need to have their columns and types specified using `AS`. You must specify at least one column and any columns used in your query. For example:

```sql
SELECT COUNT(i) FROM read_parquet('file.parquet') AS (i int);
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

#### <a name="read_csv"></a>`read_csv(path TEXT or TEXT[], /* optional parameters */) -> SETOF record`

Reads a CSV file, either from a remote location (via httpfs) or a local file.

Returns a record set (`SETOF record`). Functions that return record sets need to have their columns and types specified using `AS`. You must specify at least one column and any columns used in your query. For example:

```sql
SELECT COUNT(i) FROM read_csv('file.csv') AS (i int);
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

#### <a name="read_json"></a>`read_json(path TEXT or TEXT[], /* optional parameters */) -> SETOF record`

Reads a JSON file, either from a remote location (via httpfs) or a local file.

Returns a record set (`SETOF record`). Functions that return record sets need to have their columns and types specified using `AS`. You must specify at least one column and any columns used in your query. For example:

```sql
SELECT COUNT(i) FROM read_json('file.json') AS (i int);
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

#### <a name="iceberg_scan"></a>`iceberg_scan(path TEXT, /* optional parameters */) -> SETOF record`

Reads an Iceberg table, either from a remote location (via httpfs) or a local directory.

To use `iceberg_scan`, you must enable the `iceberg` extension:

```sql
SELECT duckdb.install_extension('iceberg');
```

Returns a record set (`SETOF record`). Functions that return record sets need to have their columns and types specified using `AS`. You must specify at least one column and any columns used in your query. For example:

```sql
SELECT COUNT(i) FROM iceberg_scan('data/iceberg/table') AS (i int);
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

#### <a name="delta_scan"></a>`delta_scan(path TEXT) -> SETOF record`

Reads a delta dataset, either from a remote (via httpfs) or a local location.

Returns a record set (`SETOF record`). Functions that return record sets need to have their columns and types specified using `AS`. You must specify at least one column and any columns used in your query. For example:

To use `delta_scan`, you must enable the `delta` extension:

```sql
SELECT duckdb.install_extension('delta');
```

```sql
SELECT COUNT(i) FROM delta_scan('/path/to/delta/dataset') AS (i int);
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

TODO

#### <a name="raw_query"></a>`duckdb.raw_query(extension_name TEXT) -> void`

TODO

#### <a name="recycle_ddb"></a>`duckdb.recycle_ddb() -> void`

TODO

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
