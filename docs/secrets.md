# Secrets Management

`pg_duckdb` provides secure credential management for accessing cloud storage, data lakes, and cloud analytics platforms. Secrets can be configured using simple utility functions or advanced Foreign Data Wrapper (FDW) configurations.

## Quick Start: Simple Secrets

The easiest way to configure credentials is using the utility functions:

### AWS S3 / Compatible Storage

```sql
-- Basic S3 secret (most common)
SELECT duckdb.create_simple_secret(
    type := 'S3',
    key_id := 'your_access_key_id',
    secret := 'your_secret_access_key',
    region := 'us-east-1'
);

-- S3 with all optional parameters
SELECT duckdb.create_simple_secret(
    type := 'S3',                             -- Required: S3, GCS, or R2
    key_id := 'your_access_key_id',           -- Required: Access key ID
    secret := 'your_secret_access_key',       -- Required: Secret access key
    region := 'us-east-1',                    -- Required: AWS region
    session_token := 'session_token',         -- Optional: For temporary credentials
    endpoint := 'https://s3.amazonaws.com',   -- Optional: For S3-compatible storage
    url_style := 'path',                      -- Optional: 'path' or 'vhost'
    use_ssl := 'true'                         -- Optional: 'true' or 'false'
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

To configure secrets for Azure Blob Storage, you must use the [Advanced Configuration](#advanced-configuration-foreign-data-wrapper) method with the `azure` secret type.

> **Note**: Azure write operations are not yet supported. See the [current discussion](https://github.com/duckdb/duckdb-azure/issues/44) for updates.

## MotherDuck

Connect to MotherDuck for cloud-scale analytics:

```sql
-- Enable MotherDuck with your token
SELECT duckdb.enable_motherduck('your_motherduck_token');

-- Or specify a specific database
SELECT duckdb.enable_motherduck('your_motherduck_token', 'my_database');

-- Check if MotherDuck is enabled
SELECT duckdb.is_motherduck_enabled();
```

Once enabled, your MotherDuck databases and tables become automatically available in PostgreSQL. See the [MotherDuck Integration guide](motherduck.md) for complete details.

## Advanced Configuration: Foreign Data Wrapper

For advanced use cases, you can define secrets using PostgreSQL's Foreign Data Wrapper (FDW) system:

### Using Credential Chain (AWS)

For AWS environments with IAM roles or instance profiles:

```sql
CREATE SERVER aws_s3_secret
TYPE 's3'
FOREIGN DATA WRAPPER duckdb
OPTIONS (provider 'credential_chain');
```

This automatically uses the AWS credential chain (environment variables, instance profile, etc.).

### Using Explicit Credentials

For explicit credential management with separate sensitive information:

```sql
-- Create server with non-sensitive options
CREATE SERVER my_s3_secret 
TYPE 's3' 
FOREIGN DATA WRAPPER duckdb
OPTIONS (
    region 'us-west-2',
    endpoint 'https://s3.amazonaws.com',
    provider 'config'
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

You can use any DuckDB secret type, provided the related extension is installed:

| Secret Type | Extension Required | Use Case |
| :---------- | :----------------- | :------- |
| `S3` | `httpfs` (pre-installed) | AWS S3, MinIO, other S3-compatible storage |
| `GCS` | `httpfs` (pre-installed) | Google Cloud Storage |
| `R2` | `httpfs` (pre-installed) | Cloudflare R2 |
| `Azure` | `azure` | Azure Blob Storage |
| Azure | azure | Azure Blob Storage |

See the [DuckDB Secrets Manager documentation](https://duckdb.org/docs/configuration/secrets_manager.html) for complete details.

## How Secrets Work

Secrets are stored using PostgreSQL's Foreign Data Wrapper system:

- **`SERVER`**: Stores non-sensitive configuration (e.g., endpoints, regions, providers).
- **`USER MAPPING`**: Stores sensitive credentials (e.g., access keys, secrets, tokens).

When a DuckDB instance is created or when secrets are modified, `pg_duckdb` automatically loads the secrets into DuckDB's secrets manager as non-persistent secrets.

## Complete Examples

### S3 Data Lake Access

Here's a complete workflow for setting up S3 access:

```sql
-- 1. Create simple secret (recommended for most users)
SELECT duckdb.create_simple_secret(
    type := 'S3',
    key_id := 'AKIAIOSFODNN7EXAMPLE',
    secret := 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    region := 'us-east-1'
);

-- 2. Test the secret by reading a file
SELECT COUNT(*) FROM read_parquet('s3://your-bucket/data.parquet');

-- 3. Query with analytical functions
SELECT 
    region, 
    date_trunc('month', order_date) as month,
    SUM(revenue) as monthly_revenue,
    approx_count_distinct(customer_id) as unique_customers
FROM read_parquet('s3://datalake/orders/year=2024/**/*.parquet')
GROUP BY ALL;

-- 4. Write processed data back to S3
COPY (SELECT * FROM my_table) TO 's3://your-bucket/export.parquet';
```

### Multi-Cloud Setup

Connect to multiple cloud providers simultaneously:

```sql
-- AWS S3
SELECT duckdb.create_simple_secret(
    type := 'S3',
    key_id := 'your_aws_key',
    secret := 'your_aws_secret',
    region := 'us-east-1'
);

-- Google Cloud Storage
SELECT duckdb.create_simple_secret(
    type := 'GCS',
    key_id := 'your_gcs_key',
    secret := 'your_gcs_secret'
);

-- Cloudflare R2
SELECT duckdb.create_simple_secret(
    type := 'R2',
    key_id := 'your_r2_key',
    secret := 'your_r2_secret',
    endpoint := 'https://your-account.r2.cloudflarestorage.com'
);

-- Now you can query across all clouds
SELECT 'AWS' as provider, COUNT(*) FROM read_parquet('s3://aws-bucket/data.parquet')
UNION ALL
SELECT 'GCS' as provider, COUNT(*) FROM read_parquet('gcs://gcs-bucket/data.parquet')
UNION ALL
SELECT 'R2' as provider, COUNT(*) FROM read_parquet('r2://r2-bucket/data.parquet');
```

### MotherDuck + S3 Hybrid

Combine cloud analytics with data lake storage:

```sql
-- 1. Enable MotherDuck
SELECT duckdb.enable_motherduck('your_token');

-- 2. Set up S3 access
SELECT duckdb.create_simple_secret(
    type := 'S3',
    key_id := 'your_aws_key',
    secret := 'your_aws_secret',
    region := 'us-east-1'
);

-- 3. Create cloud table from S3 data
CREATE TABLE cloud_analytics USING duckdb AS
SELECT * FROM read_parquet('s3://your-bucket/large-dataset/*.parquet');

-- 4. Query combines PostgreSQL, S3, and MotherDuck
SELECT 
    pg.customer_name,
    s3.historical_data,
    md.analytics_score
FROM customers pg  -- PostgreSQL table
JOIN read_parquet('s3://bucket/history.parquet') s3 ON pg.id = s3.customer_id
JOIN cloud_analytics md ON pg.id = md.customer_id;
```

## Troubleshooting

**Common Issues:**

- **Permission denied**: Verify your credentials and bucket permissions
- **Region mismatch**: Ensure the region matches your bucket's location  
- **Endpoint issues**: For S3-compatible storage, verify the endpoint URL
- **Network access**: Ensure your PostgreSQL server can reach the storage endpoints
- **Secret not found**: Check that secrets are created before attempting to use them
- **Extension missing**: Install required extensions (e.g., `azure` for Azure Blob Storage)

**Debugging Tips:**

```sql
-- List all active secrets
SELECT r['name'], r['type'], r['scope'] 
FROM duckdb.query($$SELECT name, type, scope FROM duckdb_secrets()$$) r;

-- Check if MotherDuck is enabled
SELECT duckdb.is_motherduck_enabled();

-- Verify extension installation
SELECT * FROM duckdb.extensions WHERE name IN ('httpfs', 'azure');
```

## Security Best Practices

- **Use IAM roles** when possible instead of long-lived access keys
- **Rotate credentials** regularly and update secrets accordingly
- **Limit permissions** to only the required buckets and actions
- **Use environment variables** for sensitive values in production
- **Monitor access** through cloud provider audit logs

## Further Reading

- [Functions](functions.md) - Complete function reference including secrets.
- [MotherDuck Integration](motherduck.md) - Cloud analytics integration guide.
- [DuckDB Secrets Manager](https://duckdb.org/docs/configuration/secrets_manager.html)
- [S3 API Support](https://duckdb.org/docs/extensions/httpfs/s3api.html)
- [Google Cloud Storage Import](https://duckdb.org/docs/guides/network_cloud_storage/gcs_import.html)
- [Cloudflare R2 Import](https://duckdb.org/docs/guides/network_cloud_storage/cloudflare_r2_import.html)
- [Azure Extension](https://duckdb.org/docs/extensions/azure)
