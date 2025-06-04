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

## Time Functions

|Name|Description|
| :--- | :---------- |
|[`time_bucket`](#time_bucket)|Bucket timestamps into time intervals for time-series analysis|

## DuckDB Administration Functions

| Name | Description |
| :--- | :---------- |
| [`duckdb.install_extension`](#install_extension) | Installs a DuckDB extension |
| [`duckdb.load_extension`](#load_extension) | Loads a DuckDB extension for the current session |
| [`duckdb.autoload_extension`](#autoload_extension) | Configures whether an extension should be auto-loaded |
| [`duckdb.query`](#query) | Runs a SELECT query directly against DuckDB |
| [`duckdb.raw_query`](#raw_query) | Runs any query directly against DuckDB (meant for debugging)|
| [`duckdb.recycle_ddb`](#recycle_ddb) | Force a reset the DuckDB instance in the current connection (meant for debugging) |

## Secrets Management Functions

| Name | Description |
| :--- | :---------- |
| [`duckdb.create_simple_secret`](#create_simple_secret) | Creates a simple secret for cloud storage access |

## Motherduck Functions

| Name | Description |
| :--- | :---------- |
| [`duckdb.enable_motherduck`](#enable_motherduck) | Enables MotherDuck integration with a token |
| [`duckdb.is_motherduck_enabled`](#is_motherduck_enabled) | Checks if MotherDuck integration is enabled |
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
SELECT * FROM read_json('file.json');
SELECT r['id'], r['name'] FROM read_json('file.json') r WHERE r['age'] > 21;
SELECT COUNT(*) FROM read_json('file.json');
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

#### <a name="iceberg_snapshots"></a>`iceberg_snapshots(path TEXT, /* optional parameters */) -> SETOF iceberg_snapshot_record`

Reads Iceberg snapshot information from an Iceberg table.

To use `iceberg_snapshots`, you must enable the `iceberg` extension:

```sql
SELECT duckdb.install_extension('iceberg');
```

This function returns snapshot metadata for an Iceberg table, which can be useful for time travel queries and understanding table history.

```sql
SELECT * FROM iceberg_snapshots('data/iceberg/table');
```

Further information:

* [DuckDB Iceberg extension documentation](https://duckdb.org/docs/extensions/iceberg.html)

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| path | text | The path, either to a remote httpfs location or a local location (if enabled), of the Iceberg table to read. |

##### Optional Arguments

Optional parameters mirror DuckDB's `iceberg_snapshots` function. To specify optional parameters, use `parameter := 'value'`.

| Name | Type | Default | Description |
| :--- | :--- | :------ | :---------- |
| metadata_compression_codec | text | `'none'` | |
| skip_schema_inference | boolean | false | |
| version | text | `'version-hint.text'` | |
| version_name_format | text | `'v%s%s.metadata.json,%s%s.metadata.json'` | |

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

#### <a name="install_extension"></a>`duckdb.install_extension(extension_name TEXT, repository TEXT DEFAULT 'core') -> bool`

Installs a DuckDB extension and configures it to be loaded automatically in
every session that uses pg_duckdb.

```sql
SELECT duckdb.install_extension('iceberg');
SELECT duckdb.install_extension('avro', 'community');
```

##### Security

Since this function can be used to install and download any extensions it can
only be executed by a superuser by default. To allow execution by some other
admin user, such as `my_admin`, you can grant such a user the following
permissions:

```sql
GRANT ALL ON FUNCTION duckdb.install_extension(TEXT, TEXT) TO my_admin;
```

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| extension_name | text | The name of the extension to install |

#### <a name="load_extension"></a>`duckdb.load_extension(extension_name TEXT) -> void`

Loads a DuckDB extension for the current session only. Unlike `install_extension`, this doesn't configure the extension to be loaded automatically in future sessions.

```sql
SELECT duckdb.load_extension('spatial');
```

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| extension_name | text | The name of the extension to load |

#### <a name="autoload_extension"></a>`duckdb.autoload_extension(extension_name TEXT, autoload BOOLEAN) -> void`

Configures whether an installed extension should be automatically loaded in new sessions.

```sql
-- Disable auto-loading for an extension
SELECT duckdb.autoload_extension('spatial', false);

-- Enable auto-loading for an extension
SELECT duckdb.autoload_extension('spatial', true);
```

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| extension_name | text | The name of the extension to configure |
| autoload | boolean | Whether the extension should be auto-loaded |

#### <a name="query"></a>`duckdb.query(query TEXT) -> SETOF duckdb.row`

Executes the given SELECT query directly against DuckDB. This can be useful if DuckDB syntax makes the query easier to write or if you want to use a function that is not exposed by pg_duckdb yet. If you use it because of a missing function in pg_duckdb, please also open an issue on the GitHub repository so that we can add support. For example the below query shows a query that puts `FROM` before `SELECT` and uses a list comprehension. Both of those features are not supported in Postgres.

```sql
SELECT * FROM duckdb.query('FROM range(10) as a(a) SELECT [a for i in generate_series(0, a)] as arr');
```

#### <a name="raw_query"></a>`duckdb.raw_query(query TEXT) -> void`

Runs an arbitrary query directly against DuckDB. Compared to `duckdb.query`, this function can execute any query, not just SELECT queries. The main downside is that it doesn't return its result as rows, but instead sends the query result to the logs. So the recommendation is to use `duckdb.query` when possible, but if you need to run e.g. some DDL you can use this function.

#### <a name="recycle_ddb"></a>`duckdb.recycle_ddb() -> void`

pg_duckdb keeps the DuckDB instance open inbetween transactions. This is done
to save session level state, such as manually done `SET` commands. If you want
to clear this session level state for some reason you can close the currently
open DuckDB instance using:

```sql
CALL duckdb.recycle_ddb();
```

#### <a name="enable_motherduck"></a>`duckdb.enable_motherduck(token TEXT [, database_name TEXT]) -> void`

Enables MotherDuck integration with the provided authentication token.

```sql
-- Enable MotherDuck with default database
SELECT duckdb.enable_motherduck('your_token_here');

-- Enable MotherDuck with specific database
SELECT duckdb.enable_motherduck('your_token_here', 'my_database');
```

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| token | text | Your MotherDuck authentication token |

##### Optional Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| database_name | text | Specific MotherDuck database to connect to |

#### <a name="is_motherduck_enabled"></a>`duckdb.is_motherduck_enabled() -> boolean`

Checks whether MotherDuck integration is currently enabled for this session.

```sql
SELECT duckdb.is_motherduck_enabled();
```

#### <a name="create_simple_secret"></a>`duckdb.create_simple_secret(type TEXT, key_id TEXT, secret TEXT, region TEXT [, session_token TEXT, endpoint TEXT, url_style TEXT, use_ssl TEXT]) -> void`

Creates a simple secret for accessing cloud storage services like S3, GCS, or R2.

```sql
-- Create an S3 secret
SELECT duckdb.create_simple_secret(
    type := 'S3',
    key_id := 'your_access_key',
    secret := 'your_secret_key',
    region := 'us-east-1'
);

-- Create an S3 secret with session token
SELECT duckdb.create_simple_secret(
    type := 'S3',
    key_id := 'your_access_key',
    secret := 'your_secret_key',
    region := 'us-east-1',
    session_token := 'your_session_token'
);
```

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| type | text | The type of secret ('S3', 'GCS', 'R2', etc.) |
| key_id | text | The access key ID or equivalent |
| secret | text | The secret key or equivalent |
| region | text | The region for the service |

##### Optional Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| session_token | text | Session token for temporary credentials |
| endpoint | text | Custom endpoint URL |
| url_style | text | URL style ('vhost' or 'path') |
| use_ssl | text | Whether to use SSL ('true' or 'false') |

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

#### <a name="time_bucket"></a>`time_bucket(bucket_width INTERVAL, timestamp_col TIMESTAMP [, origin TIMESTAMP]) -> TIMESTAMP`

Buckets timestamps into time intervals for time-series analysis. This function is compatible with TimescaleDB's `time_bucket` function, allowing for easier migration and interoperability.

```sql
-- Group events by hour
SELECT time_bucket(INTERVAL '1 hour', created_at) as hour_bucket, COUNT(*)
FROM events
GROUP BY hour_bucket
ORDER BY hour_bucket;

-- Group by 15-minute intervals
SELECT time_bucket(INTERVAL '15 minutes', timestamp_col), AVG(value)
FROM sensor_data
WHERE timestamp_col >= '2024-01-01'
GROUP BY 1
ORDER BY 1;
```

Further information:
* [DuckDB date_trunc function](https://duckdb.org/docs/sql/functions/dateformat.html#date_trunc)
* [TimescaleDB time_bucket function](https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/)

##### Required Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| bucket_width | interval | The interval size for bucketing (e.g., '1 hour', '15 minutes') |
| timestamp_col | timestamp | The timestamp column to bucket |

##### Optional Arguments

| Name | Type | Description |
| :--- | :--- | :---------- |
| origin | timestamp | The origin point for bucketing. Buckets are aligned to this timestamp. |

**Note**: The `time_bucket` function also supports timezone and time offset parameters for more advanced time bucketing scenarios.
