# Building from Source

This guide provides detailed instructions for building `pg_duckdb` from source on different platforms.

**First things first, clone the repo**:

```bash
git clone https://github.com/duckdb/pg_duckdb
cd pg_duckdb
```

And then you can follow the instructions for your operating system below


## Requirements

- **PostgreSQL**: 14, 15, 16, or 17
- **Operating Systems**: Ubuntu 22.04-24.04, macOS, or other UNIX-like systems
- **Build Tools**: Standard PostgreSQL extension build tools
- **DuckDB Dependencies**: [DuckDB build requirements](https://duckdb.org/docs/stable/dev/building/overview.html)

For full dependency details, see our [GitHub Actions workflow](../.github/workflows/build_and_test.yaml).

## Build Options

- **Dynamically Linked Release Build**: `make install`
- **Statically Linked Release Build**: `DUCKDB_BUILD=ReleaseStatic make install`
- **Debug Build**: `DUCKDB_BUILD=Debug make install`
- **Specific PostgreSQL Version**: `PG_CONFIG=/path/to/pg_config make install`

## Static Compilation

pg_duckdb supports statically linking the DuckDB library into the extension. This is mostly useful to ensure that the right DuckDB version if you have installed different version, and/or to avoid installation conflicts with other Postgres extensions that require different versions of DuckDB.


# Build on Ubuntu 24.04

This example uses PostgreSQL 17. If you wish to use another version, substitute the version number in the commands as necessary.

### Set up Postgres

We recommend using PGDG for Postgres, but you are welcome to use any Postgres packages or install from source. To install Postgres 17 from PGDG:

```sh
sudo apt install postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt install postgresql-17 postgresql-server-dev-17
```

If you do not install from PGDG, please note that you must have the **`server-dev`** package installed to compile extensions.

### Install Build Dependencies

```sh
sudo apt install \
    build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev \
    libxslt-dev libssl-dev libxml2-utils xsltproc pkg-config libc++-dev \
    libc++abi-dev libglib2.0-dev libtinfo6 cmake libstdc++-12-dev \
    liblz4-dev ninja-build
```

### Build and Install

```sh
make -j$(nproc)
sudo make install
```

### Add pg_duckdb to shared_preload_libraries

```sh
echo "shared_preload_libraries = 'pg_duckdb'" | sudo tee /etc/postgresql/17/main/conf.d/pg_duckdb.conf
```

Alternatively, you can directly edit `/etc/postgresql/17/main/postgresql.conf` if desired.

### Restart Postgres

```sh
sudo service postgresql restart
```

### Connect and Activate

You may wish to now create databases and users as desired. To use `pg_duckdb` immediately, you can use
the `postgres` superuser to connect to the default `postgres` database:

```console
$ sudo -u postgres psql

postgres=# CREATE EXTENSION pg_duckdb;
```

# Build on macOS

## Prerequisites

1. **Install Xcode Command Line Tools:**
   ```bash
   xcode-select --install
   ```

2. **Install Homebrew** (if not already installed):
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

3. **Install PostgreSQL:**
   ```bash
   # Install PostgreSQL (latest version)
   brew install postgresql@17

   # Start PostgreSQL service
   brew services start postgresql@17

   # Add PostgreSQL to PATH (add to ~/.zshrc or ~/.bash_profile)
   export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
   ```

## Install Build Dependencies

```bash
# Install required build tools
brew install cmake ninja pkg-config

# Install additional dependencies for DuckDB
brew install lz4
```

## Build and Install

1.  **Build and Install:**
    ```bash
    make -j$(sysctl -n hw.ncpu)
    sudo make install
    ```

2.  **Configure PostgreSQL:**
   ```bash
   # Find PostgreSQL config directory
   postgres --help-config

   # Edit postgresql.conf (adjust path as needed)
   echo "shared_preload_libraries = 'pg_duckdb'" >> /opt/homebrew/var/postgresql@17/postgresql.conf
   ```

3. **Restart PostgreSQL:**
   ```bash
   brew services restart postgresql@17
   ```

4. **Create extension:**
   ```bash
   psql -d postgres -c "CREATE EXTENSION pg_duckdb;"
   ```

## Troubleshooting macOS

- **Permission issues**: Use `sudo` for `make install`
- **Multiple PostgreSQL versions**: Set `PG_CONFIG` to the correct version:
  ```bash
  export PG_CONFIG=/opt/homebrew/opt/postgresql@17/bin/pg_config
  ```
- **Apple Silicon**: All dependencies should install natively via Homebrew

# FAQ

Q: How do I build for multiple versions of Postgres?
A: If you have multiple versions of Postgres installed, set `PG_CONFIG` to the path of the `pg_config` binary that you would like to use for building before compilation.

  ```sh
  export PG_CONFIG=/usr/bin/pg_config
  ```

Q: `make clean` didn't remove all the build artifacts. How do I clean the entire project?
A: `make clean` will clean the `pg_duckdb` build files, but not `libduckdb`, which only needs to be rebuilt on a DuckDB version change. To clean both `pg_duckdb` and `libduckdb`, use `make clean-all`.
