# pg_quack

quack is a PostgreSQL extension that embeds DuckDB into Postgres. :duck:

## Usage

```
CREATE EXTENSION quack;
```

Once loaded, DuckDB is used to execute all SELECT queries. You can disable it by
setting `quack.execution` to `false`.

Then query as usual. You can toggle DuckDB execution using `quack.execution`:

```sql
SET quack.execution = false;
```

## Features

* `SELECT` queries are optimistically executed by DuckDB; if DuckDB cannot
  support the query for any reason, execution falls back to Postgres.
* Able to read [data types][datatypes] that exist in both Postgres and DuckDB. The
  following data types are supported: numeric, character, binary, date/time,
  boolean, uuid, json, and arrays (see "Limitations").
* Add a credential to enable DuckDB's httpfs support.
  ```sql
  INSERT INTO quack.secrets
  (cloud_type, cloud_id, cloud_secret, cloud_region)
  VALUES ('S3', 'access_key_id', 'secret_accss_key', 'us-east-1')
  ```
* Read parquet and csv files
  * `SELECT n FROM read_parquet('s3://bucket/file.parquet') AS (n int)`
  * `SELECT n FROM read_csv('s3://bucket/file.csv') AS (n int)`
  * You can pass globs and arrays to these functions, just like in DuckDB
* Write a query or entire table to parquet in S3
  * `COPY (SELECT foo, bar FROM baz) TO 's3://...'`
  * `COPY table TO 's3://...'`
* Join data from files in S3 with data in your Postgres heap tables, views, and materialized views.
* Read and write Parquet in a single query:

  ```sql
  COPY (
    SELECT count(*), name
    FROM read_parquet('s3://bucket/file.parquet') AS (name text)
    GROUP BY name
    ORDER BY count DESC
  ) TO 's3://bucket/results.parquet';
  ```

## Limitations

* Only one- and two-dimensional arrays are supported. Two-dimensional arrays are limited to boolean
  and integer types.
* Data types are limited to the most commonly used data types. The following data types are not
  supported: monetary, geometric, enum, network, bit string, text search, composite, range, xml.
  These data types refer to [the Postgres datatype documentation][datatypes].
  * Arbitrary precision `numeric` types are only supported up to a width of 38 digits
* Types, functions, and casts defined by the user or by an extension are not supported

## Roadmap

Please [see the Github project](https://github.com/orgs/hydradatabase/projects/1) for upcoming planned tasks and
features.

## Installation

```
make install
```

Prerequisites:

* Postgres 16
* Ubuntu 22.04 or MacOS
* Standard set of build tools for building Postgres extensions
* [Build tools that are required to build DuckDB](https://duckdb.org/docs/dev/building/build_instructions)



[datatypes]: https://www.postgresql.org/docs/current/datatype.html
