# MotherDuck Read-Scaling

MotherDuck read-scaling enables you to handle read-heavy workloads by creating multiple read-only connections to a MotherDuck database from different PostgreSQL sessions. This feature helps avoid performance bottlenecks when connecting many concurrent users or BI tools through a single MotherDuck account.

## Overview

By default, all connections using the same MotherDuck account share a single cloud DuckDB instance (a "duckling"). Read-scaling solves this limitation by:

- **Creating Read-Only Replicas**: Spinning up multiple read-only replicas of your database
- **Distributing Load**: Each replica is powered by its own dedicated duckling
- **Scaling Automatically**: As more users connect via read-scaling tokens, your flock of ducklings expands
- **Maintaining Affinity**: Users are assigned to specific replicas for consistent performance

## How It Works

### Token Types

**Primary Token**: Standard MotherDuck token with full read-write access
**Read Scaling Token**: Special token that provides read-only access and directs connections to dedicated read replicas

### Connection Behavior

1. **Primary Connection**: Uses standard token, has full read-write access to the main duckling
2. **Read-Scaling Connections**: Use read-scaling tokens, are assigned to dedicated read-only replicas
3. **Eventual Consistency**: Read replicas sync changes from the primary instance within a few minutes
4. **Session Affinity**: Users can be consistently routed to the same replica for better caching

## Configuration

### Prerequisites

- Valid MotherDuck account and authentication tokens
- MotherDuck database created and accessible
- Multiple PostgreSQL sessions for testing read-scaling

### Creating Read Scaling Tokens

1. Generate a read-scaling token through the MotherDuck UI
2. When creating an access token, select "Read Scaling Token" as the token type
3. Read-scaling tokens grant read permissions but restrict write operations

Additional resources:

- [MotherDuck Authentication Documentation](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/)
- [MotherDuck Read Scaling](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/read-scaling)

### Setting Up Read-Scaling in pg_duckdb

#### Primary (Read-Write) Connection
```sql
-- Connect with standard token for full read-write access
CALL duckdb.enable_motherduck('<your_primary_token>', '<database_name>');
```

#### Read-Scaling (Read-Only) Connections
```sql
-- Connect with read-scaling token for read-only access
CALL duckdb.enable_motherduck('<your_read_scaling_token>', '<database_name>');
```

## Usage Examples

### Basic Read-Scaling Setup

**Session 1: Primary Connection (Read-Write)**
```sql
-- Connect with primary token
CALL duckdb.enable_motherduck('md_primary_token_here', 'sales_db');

-- Create and populate data
CREATE TABLE products(id int, name varchar, price decimal);
INSERT INTO products VALUES
    (1, 'Widget A', 19.99),
    (2, 'Widget B', 29.99),
    (3, 'Widget C', 39.99);
```

**Session 2: Read-Scaling Connection (Read-Only)**
```sql
-- Connect with read-scaling token
CALL duckdb.enable_motherduck('md_read_scaling_token_here', 'sales_db');

-- Wait for data synchronization (replicas sync within a few minutes)
SELECT duckdb.raw_query($$ REFRESH DATABASE sales_db; $$);

-- Query data (read-only access)
SELECT name, price
FROM products
WHERE price > 25.00
ORDER BY price;
```

### Advanced Data Synchronization

For applications requiring stricter synchronization, you can manually control data freshness:

**On Primary Connection:**
```sql
-- Make changes
INSERT INTO products VALUES (4, 'Widget D', 49.99);

-- Create snapshot to ensure consistency
SELECT duckdb.raw_query($$ CREATE SNAPSHOT OF sales_db; $$);
```

**On Read-Scaling Connections:**
```sql
-- Refresh to see the latest snapshot
SELECT duckdb.raw_query($$ REFRESH DATABASE sales_db; $$);
-- Or refresh all databases
SELECT duckdb.raw_query($$ REFRESH DATABASES; $$);

-- Now query the updated data
SELECT COUNT(*) FROM products; -- Will show all 4 products
```

## Session Hints and Connection Management

### Using Session Hints

To ensure users consistently connect to the same replica (improving caching and consistency), you can use session hints when connecting:

```python
# Example using session hint in connection setup
user_spec = {
    "database": "my_database",
    "token": "read_scaling_token_here",
    "hint": "user123"  # Consistent hint for this user
}
```

### Connection String Parameters

You can also configure additional parameters in MotherDuck connection strings:

- **session_hint**: Routes clients with the same hint to the same replica
- **dbinstance_inactivity_ttl**: Sets cache TTL in seconds (e.g., `md:?dbinstance_inactivity_ttl=300`)

## Limitations and Behavior

### Read-Only Restrictions

Read-scaling connections cannot perform write operations, including:

- **Data Modifications**: `INSERT`, `UPDATE`, `DELETE` operations
- **Schema Changes**: `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`
- **Database Management**: Creating new databases, attaching/detaching databases

Attempting write operations on read-scaling connections will result in an error:
```
Cannot execute statement of type "INSERT" on database "database_name" which is attached in read-only mode!
```

### Data Consistency Model

- **Eventually Consistent**: Read replicas typically sync changes from the primary instance within a few minutes
- **Lag Tolerance**: Read operations might see data that slightly lags behind the latest writes
- **Manual Refresh**: Use `REFRESH DATABASE` to ensure read-scaling connections see the latest data
- **Snapshot Control**: Create snapshots on the writer and refresh on readers for stricter consistency

### Scaling Limits

- **Default Replica Limit**: 16 read-scaling replicas by default
- **Configurable**: Contact MotherDuck support to adjust the replica limit
- **Sharing Behavior**: If the limit is exceeded, new connections share existing replicas
- **Affinity Preservation**: MotherDuck maintains user-to-replica affinity where possible
