FROM postgres:16-bookworm as base

###
### BUILDER
###
FROM base as builder

RUN --mount=type=cache,target=/var/cache/apt \
  apt-get update -qq && \
  apt-get install -y build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev \
  libssl-dev libxml2-utils xsltproc pkg-config libc++-dev libc++abi-dev libglib2.0-dev libtinfo5 cmake \
  libstdc++-12-dev postgresql-server-dev-16 liblz4-dev ccache

WORKDIR /build

ENV PATH=/usr/lib/ccache:$PATH
ENV CCACHE_DIR=/ccache

# A more selective copy might be nice, but the git submodules are not cooperative.
# Instead, use .dockerignore to not copy files here.
COPY . .

RUN make clean

# permissions so we can run as `postgres` (uid=999,gid=999)
RUN chown -R postgres:postgres .
RUN chown -R postgres:postgres /usr/lib/postgresql /usr/share/postgresql
RUN mkdir /out && chown postgres:postgres /out
RUN rm -f .depend

USER postgres
RUN --mount=type=cache,target=/ccache/,uid=999,gid=999 make install
RUN --mount=type=cache,target=/ccache/,uid=999,gid=999 DESTDIR=/out make install

###
### CHECKER
###
FROM builder as checker

USER postgres
RUN --mount=type=cache,target=/ccache/,uid=999,gid=999 make installcheck

###
### OUTPUT
###
# this creates a usable postgres image but without the packages needed to build
FROM base as output
COPY --from=builder /out /
