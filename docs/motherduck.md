# MotherDuck Integration

## Connect with MotherDuck

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

## Using `pg_duckdb` with MotherDuck

After doing the configuration (and possibly restarting Postgres). You can then you create tables in the MotherDuck database by using the `duckdb` [Table Access Method][tam] like this:

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
