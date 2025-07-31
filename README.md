<p align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="logo-dark.svg">
        <img width="800" src="logo-light.svg" alt="pg_duckdb logo" />
    </picture>
</p>

<p align="center">
    <strong>PostgreSQL extension for DuckDB</strong><br>
</p>

<p align="center">
    <a href="https://hub.docker.com/r/pgduckdb/pgduckdb"><img src="https://img.shields.io/docker/pulls/pgduckdb/pgduckdb?style=flat-square&logo=docker" alt="Docker Pulls"></a>
    <a href="https://github.com/duckdb/pg_duckdb/releases"><img src="https://img.shields.io/github/v/release/duckdb/pg_duckdb?style=flat-square&logo=github" alt="GitHub Release"></a>
    <a href="https://github.com/duckdb/pg_duckdb/blob/main/LICENSE"><img src="https://img.shields.io/github/license/duckdb/pg_duckdb?style=flat-square" alt="License"></a>
    <a href="https://discord.duckdb.org/"><img src="https://img.shields.io/discord/909674491309850675?style=flat-square&logo=discord&logoColor=white" alt="Discord"></a>
</p>

---

# pg_duckdb: Official PostgreSQL Extension for DuckDB

**pg_duckdb** integrates DuckDB's columnar-vectorized analytics engine into PostgreSQL, enabling high-performance analytics and data-intensive applications. Built in collaboration with [Hydra][hydra] and [MotherDuck][motherduck].

## Key Features

- **High-Performance Analytics**: Execute analytical queries with DuckDB's vectorized engine
- **Seamless Integration**: Query PostgreSQL tables directly from DuckDB
- **Data Lake Connectivity**: Read/write Parquet, CSV, JSON from S3, GCS, Azure, R2
- **Modern Formats**: Native support for Iceberg, Delta Lake tables and soon [DuckLake](https://ducklake.select/)
- **MotherDuck Integration**: Cloud analytics with automatic synchronization with [MotherDuck](https://motherduck.com/)
- **Advanced Types**: Support for STRUCT, MAP, UNION, arrays, and JSON
- **Extension Ecosystem**: Install DuckDB extensions (iceberg, delta, azure, etc.)

## See It In Action

### Instant Analytics

Transform your PostgreSQL into a data lake powerhouse:

```sql
-- Setup S3 access in seconds
SELECT duckdb.create_simple_secret(
    type := 'S3', key_id := 'your_key', secret := 'your_secret', region := 'us-east-1'
);

-- Query terabytes of Parquet data like local tables
SELECT 
    region, 
    date_trunc('month', order_date) as month,
    SUM(revenue) as monthly_revenue,
    approx_count_distinct(customer_id) as unique_customers
FROM read_parquet('s3://datalake/orders/year=2024/**/*.parquet')
GROUP BY ALL;
```

### Hybrid OLTP + OLAP Queries

Combine your operational PostgreSQL data with analytical cloud data:

```sql
-- Join PostgreSQL customers with historical cloud analytics
SELECT 
    c.customer_name,
    h.lifetime_value,
    h.total_orders,
    CASE WHEN h.lifetime_value > 10000 THEN 'VIP' ELSE 'Standard' END as tier
FROM customers c  -- PostgreSQL table
JOIN (
    SELECT 
        customer_id,
        SUM(amount) as lifetime_value,
        COUNT(*) as total_orders
    FROM read_parquet('s3://analytics/customer_history/*.parquet')
    GROUP BY customer_id
) h ON c.id = h.customer_id
WHERE c.status = 'active'
ORDER BY h.lifetime_value DESC;
```

### Modern DataLake Formats

Work with modern data formats like DuckLake, Iceberg and Delta Lake:

```sql
-- Query Apache Iceberg tables with time travel
SELECT duckdb.install_extension('iceberg');
SELECT * FROM iceberg_scan('s3://warehouse/sales_iceberg', version := '2024-03-15-snapshot')

-- Process Delta Lake with schema evolution
SELECT duckdb.install_extension('delta');
SELECT * FROM delta_scan('s3://lakehouse/user_events')
```

### Cloud-Native ETL Pipelines

Build powerful data pipelines that scale:

```sql
-- Transform and export analytics-ready data with complex types
CREATE TEMP TABLE customer_metrics USING duckdb AS
SELECT * FROM duckdb.query($$
  SELECT 
      customer_id,
      {'total_orders': COUNT(*), 'avg_order_value': AVG(amount)} as metrics,
      MAP(['last_order', 'first_order'], [MAX(order_date), MIN(order_date)]) as dates,
      array_agg(DISTINCT product_category) as purchased_categories
  FROM read_parquet('s3://raw/orders/**/*.parquet')
  GROUP BY customer_id
$$);

-- Export processed results back to data lake
COPY customer_metrics TO 's3://processed/customer_360/data.parquet';
```

### MotherDuck Integration

Scale to cloud instantly with MotherDuck:

```sql
-- Connect to MotherDuck
CALL duckdb.enable_motherduck('<your_token>');

-- Your existing MotherDuck tables appear automatically
SELECT region, COUNT(*) FROM my_cloud_analytics_table;

-- Create cloud tables that sync across teams
CREATE TABLE real_time_kpis USING duckdb AS
SELECT 
    date_trunc('day', created_at) as date,
    COUNT(*) as daily_signups,
    SUM(revenue) as daily_revenue
FROM user_events 
GROUP BY date;
```

## Quick Start

### Try with Hydra (Recommended)

The fastest way to get started:

```bash
pip install hydra-cli
hydra
```

### Docker

Run PostgreSQL with pg_duckdb pre-installed:

```bash
docker run -d -e POSTGRES_PASSWORD=duckdb pgduckdb/pgduckdb:16-main
```

With MotherDuck:
```bash
export MOTHERDUCK_TOKEN=<your_token>
docker run -d -e POSTGRES_PASSWORD=duckdb -e MOTHERDUCK_TOKEN pgduckdb/pgduckdb:16-main
```

### Package Managers

**pgxman (apt):**
```bash
pgxman install pg_duckdb
```

**Compile from source:**

```bash
git clone https://github.com/duckdb/pg_duckdb
cd pg_duckdb
make install
```

*See [compilation guide](docs/compilation.md) for detailed instructions.*

## Configuration

See [settings documentation](docs/settings.md) for complete configuration options.

## Documentation

| Topic | Description |
|-------|-------------|
| [Functions](docs/functions.md) | Complete function reference |
| [Types](docs/types.md) | Supported data types and advanced types usage |
| [MotherDuck](docs/motherduck.md) | Cloud integration guide |
| [Secrets](docs/secrets.md) | Credential management |
| [Extensions](docs/extensions.md) | DuckDB extension usage |
| [Transactions](docs/transactions.md) | Transaction behavior |
| [Compilation](docs/compilation.md) | Build from source |

**Note**: Advanced DuckDB types (STRUCT, MAP, UNION) require DuckDB execution context. Use `duckdb.query()` for complex type operations and `TEMP` tables for DuckDB table creation in most cases. See [Types documentation](docs/types.md) for details.

## Performance

pg_duckdb excels at:
- **Analytical Workloads**: Aggregations, window functions, complex JOINs
- **Data Lake Queries**: Scanning large Parquet/CSV files
- **Mixed Workloads**: Combining OLTP (PostgreSQL) with OLAP (DuckDB)
- **ETL Pipelines**: Transform and load data at scale

## Contributing

We welcome contributions! Please see:

- [Contributing Guidelines](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Project Milestones][milestones] for upcoming features
- [Discussions][discussions] for feature requests
- [Issues][issues] for bug reports
- [Join the DuckDB Discord community](https://discord.duckdb.org/) then chat in [the #pg_duckdb channel](https://discord.com/channels/909674491309850675/1289177578237857802).

## Support

- **Documentation**: [Complete documentation][docs]
- **Community**: [DuckDB Discord #pg_duckdb channel](https://discord.com/channels/909674491309850675/1289177578237857802)
- **Issues**: [GitHub Issues][issues]
- **Commercial**: [Hydra][hydra] and [MotherDuck][motherduck] offer commercial support

## Requirements

- **PostgreSQL**: 14, 15, 16, 17
- **Operating Systems**: Ubuntu 22.04-24.04, macOS

## License

Licensed under the [MIT License](LICENSE).

---

<p align="center">
    <strong>Built with ❤️</strong><br> in collaboration with <a href="https://hydra.so">Hydra</a> and <a href="https://motherduck.com">MotherDuck</a>
</p>

[milestones]: https://github.com/duckdb/pg_duckdb/milestones
[discussions]: https://github.com/duckdb/pg_duckdb/discussions
[issues]: https://github.com/duckdb/pg_duckdb/issues
[docs]: https://github.com/duckdb/pg_duckdb/tree/main/docs
[hydra]: https://hydra.so/
[motherduck]: https://motherduck.com/