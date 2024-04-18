# pg_quack

quack is a PostgreSQL extension that embeds DuckDB into Postgres. :duck:

Once loaded, DuckDB is used for all read queries unless disabled. In the future, more options will be available.

## Status

This project is pre-alpha. It is not currently suitable for production.

## Roadmap

This is a high-level early roadmap, subject to change. In the near future, we will provide a more detailed roadmap
via a Github Project.

* [ ] Allow DuckDB to read from heap tables. This gives a stable reference implementation and stable
      testing for query execution, as well as the capability to read from heap tables for joins.
* [ ] Implement a pluggable storage API to allow DuckDB to read from a variety of Postgres
      (or even non-Postgres) storage methods. This API will allow implementors to bypass Postgres
      to connect data directly to DuckDB.
* [ ] Allow quack to read from any implemented Postgres table access method.
* [ ] Implement a columnar datastore that supports concurrent access, is MVCC-capable and ACID-compliant,
      and can be backed by cloud storage. Connect this datastore to DuckDB for reads and Postgres
      for writes.
* [ ] Be able to intelligently execute queries via DuckDB or Postgres based on query plan or other
      factors.

## Installation

```
make install
```

Prerequisites:

* Postgres 16
* Ubuntu 22.04 or MacOS
* Standard set of build tools for building Postgres extensions
* [Build tools that are required to build DuckDB](https://duckdb.org/docs/dev/building/build_instructions)

## Usage

Currently the extension is enabled anytime it is loaded:

```
LOAD quack;
```

Then query as usual.
