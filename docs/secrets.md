# Secrets

DuckDB secrets can be configured in the `duckdb.secrets` table:

```sql
-- Session Token is Optional
INSERT INTO duckdb.secrets
(type, key_id, secret, session_token, region)
VALUES ('S3', 'access_key_id', 'secret_access_key', 'session_token', 'us-east-1');
```

TODO: document `duckdb.secrets` in full detail
