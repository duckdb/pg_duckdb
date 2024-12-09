# Transactions in pg_duckdb

Multi-statement transactions are supported in pg_duckdb. There is one important restriction on this though, which is is currently necessary to ensure the expected ACID guarantees: You cannot write to both a Postgres table and a DuckDB table in the same transaction.

Sadly, this restriction also means that running DDL (e.g. `CREATE TABLE ... USING duckdb`) is currently not supported in transactions. This is due to the fact that this requires writing to metadata tables in both Postgres and DuckDB.
