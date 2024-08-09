# pg_duckdb: Official Postgres extension for DuckDB

pg_duckdb is a Postgres extension that embeds DuckDB's columnar-vectorized analytics engine and features into Postgres. We recommend using pg_duckdb to build high performance analytics and data-intensive applications.

pg_duckdb was developed in collaboration with our partners, [Hydra](https://hydra.so) and [MotherDuck](https://motherduck.com).

## Installation

Pre-built binaries and additional installation options are coming soon.

To build pg_duckdb, you need:

* Postgres 16
* Ubuntu 22.04 or MacOS
* Standard set of build tools for building Postgres extensions
* [Build tools that are required to build DuckDB](https://duckdb.org/docs/dev/building/build_instructions)

To build and install, run:

```sh
make install
```

Next, load the pg_duckdb extension:

```sql
CREATE EXTENSION pg_duckdb;
```

**IMPORTANT:** Once loaded you can use DuckDB execution by running `SET duckdb.execution TO true`. This is _opt-in_ to avoid breaking existing queries. To avoid doing that for every session, you can configure it for a certain user by doing `ALTER USER my_analytics_user SET duckdb.execution TO true`.

## Features

- `SELECT` queries executed by the DuckDB engine can directly read Postgres tables.
	- Able to read [data types](https://www.postgresql.org/docs/current/datatype.html) that exist in both Postgres and DuckDB. The following data types are supported: numeric, character, binary, date/time, boolean, uuid, json, and arrays.
	- If DuckDB cannot support the query for any reason, execution falls back to Postgres.
- Read parquet and CSV files from object storage (AWS S3, Cloudflare R2, or Google GCS).
	- `SELECT n FROM read_parquet('s3://bucket/file.parquet') AS (n int)`
	- `SELECT n FROM read_csv('s3://bucket/file.csv') AS (n int)`
	- You can pass globs and arrays to these functions, just like in DuckDB
- Enable the DuckDB Iceberg extension using `SELECT duckdb.enable_extension('iceberg')` and read Iceberg files with `iceberg_scan`.
- Write a query — or an entire table — to parquet in object storage.
	- `COPY (SELECT foo, bar FROM baz) TO 's3://...'`
	- `COPY table TO 's3://...'`
- Read and write to Parquet format in a single query

	```sql
	COPY (
	  SELECT count(*), name
	  FROM read_parquet('s3://bucket/file.parquet') AS (name text)
	  GROUP BY name
	  ORDER BY count DESC
	) TO 's3://bucket/results.parquet';
	```

- Query and `JOIN` data in object storage with Postgres tables, views, and materialized views.
- Create indexes on Postgres tables to accelerate your DuckDB queries
- Install DuckDB extensions using `SELECT duckdb.install_extension('extension_name');`
- Toggle DuckDB execution on/off with a setting:
	- `SET duckdb.execution = true|false`
- Cache remote object localy for faster execution using `SELECT duckdb.cache('path', 'type');` where
	- 'path' is HTTPFS/S3/GCS/R2 remote object
	- 'type' specify remote object type: 'parquet' or 'csv'

## Getting Started

The best way to get started is to connect Postgres to a new or existing object storage bucket (AWS S3, Cloudflare R2, or Google GCS) with pg_duckdb. You can query data in Parquet, CSV, and Iceberg format using `read_parquet`, `read_csv`, and `iceberg_scan` respectively.

1. Add a credential to enable DuckDB's httpfs support.

	```sql
	INSERT INTO duckdb.secrets
	(cloud_type, cloud_id, cloud_secret, cloud_region)
	VALUES ('S3', 'access_key_id', 'secret_accss_key', 'us-east-1');
	```

2. Copy data directly to your bucket - no ETL pipeline!

	```sql
	COPY (SELECT user_id, item_id, price, purchased_at FROM purchases)
	TO 's3://your-bucket/purchases.parquet;
	```

3. Perform analytics on your data.

	```sql
	SELECT SUM(price) AS total, item_id
	FROM read_parquet('s3://your-bucket/purchases.parquet')
	  AS (price float, item_id int)
	GROUP BY item_id
	ORDER BY total DESC
	LIMIT 100;
	```

## Roadmap

Please see the [project roadmap][roadmap] for upcoming planned tasks and features.

### Connect with MotherDuck

pg_duckdb integration with MotherDuck will enable hybrid execution with Differential Storage.

* Zero-copy snapshots and forks
* Time travel
* Data tiering
* Improved concurrency and cacheability

## Contributing

pg_duckdb was developed in collaboration with our partners, [Hydra](https://hydra.so) and [MotherDuck](https://motherduck.com). We look forward to their continued contributions and leadership.

[Hydra](https://hydra.so) is a Y Combinator-backed database company, focused on DuckDB-Powered Postgres for app developers.

[MotherDuck](https://motherduck.com) is the cloud-based data warehouse that extends the power of DuckDB.

We welcome all contributions big and small:

- Vote on or suggest features for our roadmap.
- Open a PR.
- Submit a feature request or bug report.

## Resources

- Please see the [project roadmap][roadmap] for upcoming planned tasks and features.
- [GitHub Issues](https://github.com/duckdb/pg_duckdb/issues) for bugs and missing features
- [Discord discussion](https://discord.duckdb.org/) with the DuckDB community
- See our docs for more info and limitations

[roadmap]: https://github.com/orgs/duckdb/projects/10
