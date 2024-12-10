# Compilation

To build pg_duckdb, you need:

* Postgres 14-17
* Ubuntu 22.04-24.04 or MacOS
* Standard set of build tools for building Postgres extensions
* [Build tools that are required to build DuckDB](https://duckdb.org/docs/dev/building/build_instructions)
* For full details on required dependencies you can check out our [Github Action](../.github/workflows/build_and_test.yaml).

To build and install, run:

```sh
make install
```

Add `pg_duckdb` to the `shared_preload_libraries` in your `postgresql.conf` file:

```ini
shared_preload_libraries = 'pg_duckdb'
```

Next, create the `pg_duckdb` extension:

```sql
CREATE EXTENSION pg_duckdb;
```

# Ubuntu 24.04

This example uses Postgres 17. If you wish to use another version, substitute the version number in the commands as necessary.

### Set up Postgres

We recommend using PGDG for Postgres, but you are welcome to use any Postgres packages or install from source. To install Postgres 17 from PGDG:

```sh
sudo apt install postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt install postgresql-17 postgresql-server-dev-17
```

If you do not install from PGDG, please note that you must have the `dev` package installed to compile extensions.

### Install Build Dependencies

```sh
sudo apt install \
    build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev \
    libxslt-dev libssl-dev libxml2-utils xsltproc pkg-config libc++-dev \
    libc++abi-dev libglib2.0-dev libtinfo6 cmake libstdc++-12-dev \
    liblz4-dev ninja-build
```

### Clone, Build, and Install pg_duckdb

```sh
git clone https://github.com/duckdb/pg_duckdb
cd pg_duckdb
```

```sh
make -j16
sudo make install
```

### Add pg_duckdb to shared_preload_libraries

```sh
sudo -s
echo "shared_preload_libraries = 'pg_duckdb'" >/etc/postgresql/17/main/conf.d/pg_duckdb.conf
exit
```

Alternatively, you can directly edit `/etc/postgresql/17/main/postgresql.conf` if desired.

### Restart Postgres

```sh
sudo service postgresql restart
```

### Connect and Activate

You may wish to now create databases and users as desired. To use pg_duckdb immediately, you can use
the `postgres` superuser to connect to the default `postgres` database:

```console
$ sudo -u postgres psql

postgres=# CREATE EXTENSION pg_duckdb;
```

# MacOS

TODO
