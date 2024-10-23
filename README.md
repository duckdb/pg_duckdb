<p align="center">
    <img width="500" src="logo.png" alt="temporary logo" />
</p>

# pg_duckdb: Official Postgres extension for DuckDB

pg_duckdb is a Postgres extension that embeds DuckDB's columnar-vectorized analytics engine and features into Postgres. We recommend using pg_duckdb to build high performance analytics and data-intensive applications.

pg_duckdb was developed in collaboration with our partners, [Hydra][] and [MotherDuck][].

## Features

- `SELECT` queries executed by the DuckDB engine can directly read Postgres tables. (If you only query Postgres tables you need to run `SET duckdb.force_execution TO true`, see the **IMPORTANT** section above for details)
	- Able to read [data types](https://www.postgresql.org/docs/current/datatype.html) that exist in both Postgres and DuckDB. The following data types are supported: numeric, character, binary, date/time, boolean, uuid, json, and arrays.
	- If DuckDB cannot support the query for any reason, execution falls back to Postgres.
- Read and Write support for object storage (AWS S3, Cloudflare R2, or Google GCS):
	- Read parquet and CSV files:
		- `SELECT n FROM read_parquet('s3://bucket/file.parquet') AS (n int)`
		- `SELECT n FROM read_csv('s3://bucket/file.csv') AS (n int)`
		- You can pass globs and arrays to these functions, just like in DuckDB
	- Enable the DuckDB Iceberg extension using `SELECT duckdb.install_extension('iceberg')` and read Iceberg files with `iceberg_scan`.
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
- Read and Write support for data stored in MotherDuck
- Query and `JOIN` data in object storage/MotherDuck with Postgres tables, views, and materialized views.
- Create temporary tables in DuckDB its columnar storage format using `CREATE TEMP TABLE ... USING duckdb`.
- Install DuckDB extensions using `SELECT duckdb.install_extension('extension_name');`
- Toggle DuckDB execution on/off with a setting:
	- `SET duckdb.force_execution = true|false`
- Cache remote object locally for faster execution using `SELECT duckdb.cache('path', 'type');` where
	- 'path' is HTTPFS/S3/GCS/R2 remote object
	- 'type' specify remote object type: 'parquet' or 'csv'

## Installation

### Docker

Docker images are [available on Dockerhub](https://hub.docker.com/r/pgduckdb/pgduckdb) and are based on the official Postgres image. Use of this image is [the same as the Postgres image](https://hub.docker.com/_/postgres/). For example, you can run the image directly:

```shell
docker run -d -e POSTGRES_PASSWORD=duckdb pgduckdb/pgduckdb:16-main
```

Or you can use the docker compose in this repo:

```shell
git clone https://github.com/duckdb/pg_duckdb && cd pg_duckdb && docker compose up -d
```

Once started, connect to the database using psql:

```shell
psql postgres://postgres:duckdb@127.0.0.1:5432/postgres
# Or if using docker compose
docker compose exec db psql
```

For other usages see our [Docker specific README][docker readme].

[docker readme]: https://github.com/duckdb/pg_duckdb/blob/main/docker/README.md
### pgxman (apt)

Pre-built apt binaries are [available via pgxman](https://pgx.sh/pg_duckdb). After installation, you will need to add pg_duckdb to `shared_preload_libraries` and create the extension.

```shell
pgxman install pg_duckdb
```

Note: due to the use of `shared_preload_libraries`, pgxman's container support is not currently compatible with pg_duckdb.

### Compile from source

To build pg_duckdb, you need:

* Postgres 15-17
* Ubuntu 22.04-24.04 or MacOS
* Standard set of build tools for building Postgres extensions
* [Build tools that are required to build DuckDB](https://duckdb.org/docs/dev/building/build_instructions)

To build and install, run:

```sh
make install
```

Add `pg_duckdb` to the `shared_preload_libraries` in your `postgresql.conf` file:

```ini
shared_preload_libraries = 'pg_duckdb'
```

Next, create the `pg_duckdb` extension:

```sql
CREATE EXTENSION pg_duckdb;
```

**IMPORTANT:** DuckDB execution is usually enabled automatically when needed. It's enabled whenever you use DuckDB functions (such as `read_csv`), when you query DuckDB tables, and when running `COPY table TO 's3://...'`. However, if you want queries which only touch Postgres tables to use DuckDB execution you need to run `SET duckdb.force_execution TO true`'. This feature is _opt-in_ to avoid breaking existing queries. To avoid doing that for every session, you can configure it for a certain user by doing `ALTER USER my_analytics_user SET duckdb.force_execution TO true`.

## Getting Started

pg_duckdb relies on DuckDB's vectorized execution engine to read and write data to object storage bucket (AWS S3, Cloudflare R2, or Google GCS) and/or MotherDuck. The follow two sections describe how to get started with these destinations.

### Object storage bucket (AWS S3, Cloudflare R2, or Google GCS)

Querying data stored in Parquet, CSV, and Iceberg format can be done with `read_parquet`, `read_csv`, and `iceberg_scan` respectively.

1. Add a credential to enable DuckDB's httpfs support.

	```sql
	-- Session Token is Optional
	INSERT INTO duckdb.secrets
	(type, key_id, secret, session_token, region)
	VALUES ('S3', 'access_key_id', 'secret_access_key', 'session_token', 'us-east-1');
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

### Connect with MotherDuck

pg_duckdb also integrates with [MotherDuck][md]. To enable this support you first need to [generate an access token][md-access-token] and then add the following line to your `postgresql.conf` file:

```ini
duckdb.motherduck_token = 'your_access_token'
```

NOTE: If you don't want to store the token in your `postgresql.conf`file can also store the token in the `motherduck_token` environment variable and then explicitly enable MotherDuck support in your `postgresql.conf` file:

```ini
duckdb.motherduck_enabled = true
```

If you installed `pg_duckdb` in a different Postgres database than the default one named `postgres`, then you also need to add the following line to your `postgresql.conf` file:

```ini
duckdb.motherduck_postgres_database = 'your_database_name'
```

After doing this (and possibly restarting Postgres). You can then you create tables in the MotherDuck database by using the `duckdb` [Table Access Method][tam] like this:
```sql
CREATE TABLE orders(id bigint, item text, price NUMERIC(10, 2)) USING duckdb;
CREATE TABLE users_md_copy USING duckdb AS SELECT * FROM users;
```

[tam]: https://www.postgresql.org/docs/current/tableam.html


Any tables that you already had in MotherDuck are automatically available in Postgres. Since DuckDB and MotherDuck allow accessing multiple databases from a single connection and Postgres does not, we map database+schema in DuckDB to a schema name in Postgres.

This is done in the following way:
1. Each schema in your default MotherDuck database are simply merged with the Postgres schemas with the same name.
2. Except for the `main` DuckDB schema in your default database, which is merged with the Postgres `public` schema.
3. Tables in other databases are put into dedicated DuckDB-only schemas. These schemas are of the form `ddb$<duckdb_db_name>$<duckdb_schema_name>` (including the literal `$` characters).
4. Except for the `main` schema in those other databases. That schema should be accessed using the shorter name `ddb$<db_name>` instead.

An example of each of these cases is shown below:

```sql
INSERT INTO my_table VALUES (1, 'abc'); -- inserts into my_db.main.my_table
INSERT INTO your_schema.tab1 VALUES (1, 'abc'); -- inserts into my_db.your_schema.tab1
SELECT COUNT(*) FROM ddb$my_shared_db.aggregated_order_data; -- reads from my_shared_db.main.aggregated_order_data
SELECT COUNT(*) FROM ddb$sample_data$hn.hacker_news; -- reads from sample_data.hn.hacker_news
```

[md]: https://motherduck.com/
[md-access-token]: https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#authentication-using-an-access-token

## Roadmap

Please see the [project milestones][milestones] for upcoming planned tasks and features.

## Contributing

pg_duckdb was developed in collaboration with our partners, [Hydra][] and [MotherDuck][]. We look forward to their continued contributions and leadership.

[Hydra][] is a Y Combinator-backed database company, focused on DuckDB-Powered Postgres for app developers.

[MotherDuck][] is the cloud-based data warehouse that extends the power of DuckDB.

We welcome all contributions big and small:

- [Vote on or suggest features][discussions] for our roadmap.
- [Open a PR][prs].
- [Submit a feature request or bug report][issues].

## Resources

- Please see the [project milestones][milestones] for upcoming planned tasks and features.
- [GitHub Issues][issues] for bug reports
- [Join the DuckDB Discord community](https://discord.duckdb.org/) then chat in [the #pg_duckdb channel](https://discord.com/channels/909674491309850675/1289177578237857802).

[milestones]: https://github.com/duckdb/pg_duckdb/milestones
[discussions]: https://github.com/duckdb/pg_duckdb/discussions
[prs]: https://github.com/duckdb/pg_duckdb/pulls
[issues]: https://github.com/duckdb/pg_duckdb/issues
[Hydra]: https://hydra.so/
[Motherduck]: https://motherduck.com/
