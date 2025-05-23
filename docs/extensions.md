# pg_duckdb Extensions

The following extensions are installed by default:

* httpfs
* json

Supported extensions for installation are:

* iceberg
* delta

Installing other extensions may work, but is at your own risk.

## Installing an extension

By default known extensions are allowed to be automatically installed and loaded when a DuckDB query depends on them. This behaviour can be configured using the [`duckdb.autoinstall_known_extensions`](settings.md#duckdbautoinstall_known_extensions) and [`duckdb.autoload_known_extensions`](settings.md#duckdbautoload_known_extensions) settings.

It's also possible to manually install an extension. This can be useful when this autoinstall/autoload behaviour is disabled, or when DuckDB fails to realise an extension is necessary to execute the query. Installing an extension requires superuser.

```sql
SELECT duckdb.install_extension('extname');
```

## Implementation

Installing an extension causes it to be loaded and installed globally for any connection that uses DuckDB. The current list of installed extensions is maintained in the `duckdb.extensions` table. Superusers can use this table to view, disable, or uninstall extensions, as follows:

```sql
-- Install an extension
SELECT duckdb.install_extension('iceberg');
-- view currently installed extensions
SELECT * FROM duckdb.extensions;
-- disable or enable an extension
UPDATE duckdb.extensions SET enabled = (false|true) WHERE name = 'iceberg';
-- remove an extension
DELETE FROM duckdb.extensions WHERE name = 'iceberg';
-- You can also install community extensions
SELECT duckdb.install_extension('prql', 'community');
```

There is currently no practical difference between a disabled and uninstalled extension.

## Supported Extensions

You can install any extension DuckDB extension, but you might run into various issues when trying to use them from Postgres. Often you should be able to work around such issues by using `duckdb.query` or `duckdb.raw_query`. For some extensions pg_duckdb has added dedicated support to Postgres. These extensions are listed below.

### `iceberg`

Iceberg support adds functions to read Iceberg tables and metadata. For a list of iceberg functions, see [pg_duckdb Functions](functions.md).

### `delta`

Delta support adds the ability to read Delta Lake files via [delta_scan](functions.md#delta_scan).
