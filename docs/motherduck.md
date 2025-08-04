# MotherDuck Integration

## Connect with MotherDuck

`pg_duckdb` integrates natively with [MotherDuck][md]. To enable this support, you first need to [generate an access token][md-access-token]. Then, you can enable it using the `duckdb.enable_motherduck` convenience function:

```sql
-- If not provided, the token will be read from the `motherduck_token` environment variable
-- If not provided, the default MD database name is `my_db`
CALL duckdb.enable_motherduck('<optional token>', '<optional MD database name>');
```

This function creates a `motherduck` `SERVER` using the `pg_duckdb` Foreign Data Wrapper, which hosts the options for this integration. It also provides a `USER MAPPING` for the current user, which stores the provided MotherDuck token (if any).

You can refer to the [Advanced MotherDuck Configuration](#advanced-motherduck-configuration) section below for more on the `SERVER` and `USER MAPPING` configuration.

### Non-Superuser Configuration

If you want to use MotherDuck as a non-superuser, you also have to configure the `duckdb.postgres_role` setting:

```ini
duckdb.postgres_role = 'your_role_name'  # e.g., duckdb or duckdb_group
```

You also need to ensure that this role has `CREATE` permissions on the `public` schema in Postgres, as this is where tables from the MotherDuck `main` schema are created. You can grant these permissions as follows:

```sql
GRANT CREATE ON SCHEMA public TO {your_role_name};
-- So if you've configured the duckdb role above
GRANT CREATE ON SCHEMA public TO duckdb;
```

If you grant these permissions after starting Postgres, the initial sync of MotherDuck tables may have failed for the `public` schema. You can force a full resync of the tables by running:

```sql
SELECT * FROM pg_terminate_backend((
  SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_duckdb sync worker'
));
```

## Using MotherDuck with `pg_duckdb`

After completing the configuration (and possibly restarting Postgres), you can create tables in your MotherDuck database using the `duckdb` [Table Access Method (TAM)][tam]:

```sql
CREATE TABLE orders(id bigint, item text, price NUMERIC(10, 2)) USING duckdb;
CREATE TABLE users_md_copy USING duckdb AS SELECT * FROM users;
```

[tam]: https://www.postgresql.org/docs/current/tableam.html

Any tables that you already had in MotherDuck are automatically available in Postgres. Since DuckDB and MotherDuck allow accessing multiple databases from a single connection and Postgres does not, we map database+schema in DuckDB to a schema name in Postgres.

The default MotherDuck database will be easiest to use (see below for details), by default this is `my_db`.

## Advanced MotherDuck Configuration

If you want to specify which MotherDuck database is your default, you need to configure MotherDuck using a `SERVER` and a `USER MAPPING`:

```sql
CREATE SERVER motherduck
TYPE 'motherduck'
FOREIGN DATA WRAPPER duckdb
OPTIONS (default_database '<your database>');

-- You may use `::FROM_ENV::` to have the token be read from the environment variable
CREATE USER MAPPING FOR CURRENT_USER SERVER motherduck OPTIONS (token '<your token>')
```

Note: The `duckdb.enable_motherduck` function simplifies this process:
```sql
CALL duckdb.enable_motherduck('<token>', '<default database>');
```

## Schema Mapping

DuckDB and Postgres have different schema and database conventions. The mapping from a DuckDB `database.schema` to a Postgres schema is done as follows:

1. Each schema in your default MotherDuck database is merged with the Postgres schema of the same name.
2. The `main` DuckDB schema in your default database is merged with the Postgres `public` schema.
3. Tables in other databases are placed in dedicated schemas of the form `ddb$<duckdb_db_name>$<duckdb_schema_name>` (including the literal `$` characters).
4. The `main` schema in other databases can be accessed using the shorter name `ddb$<db_name>`.

An example of each of these cases is shown below:

```sql
INSERT INTO my_table VALUES (1, 'abc'); -- inserts into my_db.main.my_table
INSERT INTO your_schema.tab1 VALUES (1, 'abc'); -- inserts into my_db.your_schema.tab1
SELECT COUNT(*) FROM ddb$my_shared_db.aggregated_order_data; -- reads from my_shared_db.main.aggregated_order_data
SELECT COUNT(*) FROM ddb$sample_data$hn.hacker_news; -- reads from sample_data.hn.hacker_news
```

## Debugging

If some tables or schemas are not appearing as expected, check your Postgres log file. The background worker that automatically syncs tables may have encountered an error, which will be reported in the logs, often with information on how to resolve the issue.

[md]: https://motherduck.com/
[md-access-token]: https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#authentication-using-an-access-token
