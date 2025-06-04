# Data Lake Secrets Management

pg_duckdb provides secure credential management for accessing cloud storage and data lakes. Secrets can be configured using simple utility functions or advanced Foreign Data Wrapper configurations.

## Quick Start: Simple Secrets

The easiest way to configure credentials is using the utility functions:

### AWS S3 / Compatible Storage

```sql
SELECT duckdb.create_simple_secret(
    type          := 'S3',                    -- Type: S3, GCS, or R2
    key_id        := 'your_access_key_id',
    secret        := 'your_secret_access_key',
    session_token := 'session_token',         -- (optional, for temporary credentials)
    region        := 'us-east-1',             -- (optional, AWS region)
    endpoint      := 'https://s3.amazonaws.com', -- (optional, for S3-compatible storage)
    url_style     := 'path'                   -- (optional, 'path' or 'vhost')
);
```

### Google Cloud Storage

```sql
SELECT duckdb.create_simple_secret(
    type     := 'GCS',
    key_id   := 'your_access_key_id',
    secret   := 'your_secret_access_key'
);
```

### Cloudflare R2

```sql
SELECT duckdb.create_simple_secret(
    type     := 'R2',
    key_id   := 'your_access_key_id',
    secret   := 'your_secret_access_key',
    endpoint := 'https://your-account.r2.cloudflarestorage.com'
);
```

### Azure Blob Storage

```sql
SELECT duckdb.create_azure_secret('<your_connection_string>');
```

**Note**: Azure write operations are not yet supported. See the [current discussion](https://github.com/duckdb/duckdb-azure/issues/44) for updates.

## Advanced Configuration: Foreign Data Wrapper

For advanced use cases, you can define secrets using PostgreSQL's Foreign Data Wrapper system:

### Using Credential Chain (AWS)

For AWS environments with IAM roles or instance profiles:

```sql
CREATE SERVER aws_s3_secret
TYPE 's3'
FOREIGN DATA WRAPPER duckdb
OPTIONS (PROVIDER 'credential_chain');
```

This automatically uses AWS credential chain (environment variables, instance profile, etc.).

### Using Explicit Credentials

For explicit credential management with separate sensitive information:

```sql
-- Create server with non-sensitive options
CREATE SERVER my_s3_secret 
TYPE 's3' 
FOREIGN DATA WRAPPER duckdb
OPTIONS (
    region 'us-west-2',
    endpoint 'https://s3.amazonaws.com'
);

-- Create user mapping with sensitive credentials
CREATE USER MAPPING FOR CURRENT_USER 
SERVER my_s3_secret
OPTIONS (
    key_id 'your_access_key_id', 
    secret 'your_secret_access_key'
);
```

### Environment Variables

Credentials can also be read from environment variables:

```sql
-- Use ::FROM_ENV:: to read from environment variables
CREATE USER MAPPING FOR CURRENT_USER 
SERVER my_s3_secret
OPTIONS (
    key_id '::FROM_ENV::',     -- Reads from AWS_ACCESS_KEY_ID
    secret '::FROM_ENV::'      -- Reads from AWS_SECRET_ACCESS_KEY
);
```

## Supported Secret Types

You can use any DuckDB secret type, provided the related extension is installed. See the [DuckDB Secrets Manager documentation](https://duckdb.org/docs/configuration/secrets_manager.html) for complete details.

## How Secrets Work

Secrets are stored using PostgreSQL's Foreign Data Wrapper system:

- **SERVER**: Stores non-sensitive configuration (endpoints, regions, providers)
- **USER MAPPING**: Stores sensitive credentials (access keys, secrets, tokens)

When a DuckDB instance is created or when secrets are modified, pg_duckdb automatically loads the secrets into DuckDB's secrets manager as non-persistent secrets.

## Complete Example

Here's a complete workflow for setting up S3 access:

```sql
-- 1. Create simple secret (recommended for most users)
SELECT duckdb.create_simple_secret(
    type   := 'S3',
    key_id := 'AKIAIOSFODNN7EXAMPLE',
    secret := 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    region := 'us-east-1'
);

-- 2. Test the secret by reading a file
SELECT COUNT(*) FROM read_parquet('s3://your-bucket/data.parquet');

-- 3. Write data to S3
COPY (SELECT * FROM my_table) TO 's3://your-bucket/export.parquet';
```

## Troubleshooting

**Common Issues:**

- **Permission denied**: Verify your credentials and bucket permissions
- **Region mismatch**: Ensure the region matches your bucket's location
- **Endpoint issues**: For S3-compatible storage, verify the endpoint URL
- **Network access**: Ensure your PostgreSQL server can reach the storage endpoints

## Further Reading

- [DuckDB Secrets Manager](https://duckdb.org/docs/configuration/secrets_manager.html)
- [S3 API Support](https://duckdb.org/docs/extensions/httpfs/s3api.html)
- [Google Cloud Storage Import](https://duckdb.org/docs/guides/network_cloud_storage/gcs_import.html)
- [Cloudflare R2 Import](https://duckdb.org/docs/guides/network_cloud_storage/cloudflare_r2_import.html)
- [Azure Extension](https://duckdb.org/docs/extensions/azure)
