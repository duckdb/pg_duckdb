FROM postgres_base AS base

###
### BUILDER
###
FROM base AS builder
ARG POSTGRES_VERSION

RUN apt-get update -qq && \
    apt-get install -y \
    postgresql-server-dev-${POSTGRES_VERSION} \
    build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev \
    libssl-dev libxml2-utils xsltproc pkg-config libc++-dev libc++abi-dev libglib2.0-dev \
    libtinfo5 cmake libstdc++-12-dev liblz4-dev ccache ninja-build git libcurl4-openssl-dev clang-14 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

ENV PATH=/usr/lib/ccache:$PATH
ENV CCACHE_DIR=/ccache
# Build DuckDB and its extensions with clang: the httpfs extension does not compile with bookworm's GCC 12 in C++17
# mode (it returns a unique_ptr of a derived type, which needs the implicit move rules of GCC 13+ or clang).
# pg_duckdb itself is still built with GCC, like Postgres.
ENV DUCKDB_CC=/usr/bin/clang-14 DUCKDB_CXX=/usr/bin/clang++-14

# permissions so we can run as `postgres` (uid=999,gid=999)
RUN mkdir /out
RUN chown -R postgres:postgres . /usr/lib/postgresql /usr/share/postgresql /out
USER postgres

# Selectively copy the files that we need. Sadly we need separate COPY commands
# for each directory, because docker puts only the contents of the source
# directory into the target directory, and not the directory itself too.
COPY --chown=postgres:postgres Makefile Makefile.global pg_duckdb.control ./
COPY --chown=postgres:postgres .git/modules/third_party/duckdb/HEAD .git/modules/third_party/duckdb/HEAD
COPY --chown=postgres:postgres sql sql
COPY --chown=postgres:postgres src src
COPY --chown=postgres:postgres include include
COPY --chown=postgres:postgres third_party third_party
COPY --chown=postgres:postgres test test

RUN make clean-all

# build
RUN --mount=type=cache,target=/ccache/,uid=999,gid=999 echo "Available CPUs=$(nproc)" && make -j$(nproc)
# install into location specified by pg_config for tests
RUN make install
# install into /out for packaging
RUN DESTDIR=/out make install

###
### CHECKER
###
FROM builder AS checker

USER postgres
RUN make installcheck

###
### OUTPUT
###
# this creates a usable postgres image but without the packages needed to build
FROM base AS output

RUN apt-get update -qq && \
    apt-get install -y ca-certificates libcurl4

# Automatically enable pg_duckdb
RUN echo "shared_preload_libraries='pg_duckdb'" >> /usr/share/postgresql/postgresql.conf.sample
COPY --chown=postgres:postgres docker/init.d/ /docker-entrypoint-initdb.d/

COPY --from=builder /out /
USER postgres

###
### OUTPUT-CNPG
###
### this creates a postgres extension image that can be used with CNPG without any tools
FROM scratch AS output-cnpg
ARG POSTGRES_VERSION

COPY --from=builder /out/usr/lib/postgresql/${POSTGRES_VERSION}/lib /lib
COPY --from=builder /out/usr/share/postgresql/${POSTGRES_VERSION}/extension /share/extension
