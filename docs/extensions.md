# pg_duckdb Extensions

The following extensions are installed by default:

* httpfs
* json

Supported core extensions for installation are:

* **iceberg** - Apache Iceberg support
* **delta** - Delta Lake support
* **azure** - Azure Blob Storage connectivity
* **spatial** - Geospatial functions and types
* **httpfs** - HTTP/S3 file system support

Community extensions are also supported (requires configuration). Installing other extensions may work, but is at your own risk.

## Installing an extension

By default known extensions are allowed to be automatically installed and loaded when a DuckDB query depends on them. This behaviour can be configured using the [`duckdb.autoinstall_known_extensions`](settings.md#duckdbautoinstall_known_extensions) and [`duckdb.autoload_known_extensions`](settings.md#duckdbautoload_known_extensions) settings.

It's also possible to manually install an extension. This can be useful when this autoinstall/autoload behaviour is disabled, or when DuckDB fails to realise an extension is necessary to execute the query. Installing an extension requires superuser.

```sql
SELECT duckdb.install_extension('extname');
```

## Community Extensions

Community extensions can be installed when `duckdb.allow_community_extensions` is enabled. This requires superuser privileges to configure for security reasons.

```sql
-- Enable community extensions (superuser required)
SET duckdb.allow_community_extensions = true;

-- Install a community extension
SELECT duckdb.install_extension('prql', 'community');
```

**Note**: In some environments, you may also need to enable unsigned extensions:

```sql
SET duckdb.allow_unsigned_extensions = true;
```

## Implementation

Installing an extension causes it to be loaded and installed globally for any connection that uses DuckDB. The current list of installed extensions is maintained in the `duckdb.extensions` table. Superusers can use this table to view, disable, or uninstall extensions, as follows:

```sql
-- Install an extension
SELECT duckdb.install_extension('iceberg');
-- view currently installed extensions
SELECT * FROM duckdb.extensions;
-- Change an extension to stop being automatically loaded in new connections
SELECT duckdb.autoload_extension('iceberg', false);
-- For such extensions, you can still load them manually in a session
SELECT duckdb.load_extension('iceberg');
-- You can also install community extensions
SELECT duckdb.install_extension('prql', 'community');
```

## Supported Extensions

You can install any DuckDB extension, but you might run into various issues when trying to use them from PostgreSQL. Often you should be able to work around such issues by using `duckdb.query` or `duckdb.raw_query`. For some extensions pg_duckdb has added dedicated support to PostgreSQL. These extensions are listed below.

### Core Extensions

#### `httpfs`
Enables reading from HTTP/HTTPS URLs and cloud storage (S3, GCS, R2). Pre-installed by default.

#### `json` 
Provides DuckDB JSON functions and operators. Pre-installed by default.

#### `azure`
Allows reading files from Azure Blob Storage using `az://...` filepaths.

#### `iceberg`
Apache Iceberg support adds functions to read Iceberg tables and metadata. For a complete list of iceberg functions, see [pg_duckdb Functions](functions.md).

#### `delta`
Delta Lake support adds the ability to read Delta Lake files via [delta_scan](functions.md#delta_scan).

#### `spatial`
Geospatial functions and data types for working with geometric data.

### Extension Usage Examples

```sql
-- Install and use Iceberg
SELECT duckdb.install_extension('iceberg');
SELECT * FROM iceberg_scan('s3://bucket/iceberg-table/');

-- Install and use Delta
SELECT duckdb.install_extension('delta');
SELECT * FROM delta_scan('s3://bucket/delta-table/');

-- Install spatial extension for geospatial queries
SELECT duckdb.install_extension('spatial');
SELECT ST_Distance(point1, point2) FROM locations;
```

## Security considerations

By default execution `duckdb.install_extension` and `duckdb.autoload_extension` is only allowed for superusers. This is to prevent users from installing extensions that may have security implications or that may interfere with the database's operation.

That means that users can only use extensions that DuckDB has marked as "auto-installable". If you want to restrict the use of those extensions as well to a specific list of allowed extensions, you can do so by setting the [`duckdb.autoinstall_known_extensions`](settings.md#duckdbautoload_known_extensions) to `false`. This will prevent users from automatically install any known extensions. Note that this requires that any of the extensions you **do** want to allow are already installed by a superuser.
