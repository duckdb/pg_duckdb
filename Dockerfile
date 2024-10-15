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
    libtinfo5 cmake libstdc++-12-dev liblz4-dev ccache ninja-build && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /build

ENV PATH=/usr/lib/ccache:$PATH
ENV CCACHE_DIR=/ccache

# A more selective copy might be nice, but the git submodules are not cooperative.
# Instead, use .dockerignore to not copy files here.
COPY . .

RUN make clean-all

# permissions so we can run as `postgres` (uid=999,gid=999)
RUN mkdir /out
RUN chown -R postgres:postgres . /usr/lib/postgresql /usr/share/postgresql /out

USER postgres
# build
RUN --mount=type=cache,target=/ccache/,uid=999,gid=999 make -j$(nproc)
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
COPY --from=builder /out /