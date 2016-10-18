FROM postgres:9.5.4

RUN mkdir /src
COPY . /src/pgoutput
# Contents of "scripts" fix pg_hba.conf and postgresql.conf
COPY scripts /docker-entrypoint-initdb.d

# Deliberately run in multiple steps.  This way we can harness intermediate
# images, and not have to re-install the build tools after the first build
RUN \
     apt-get update \
  && apt-get install -y wget gcc make pkg-config libprotobuf-dev postgresql-server-dev-9.5

# Debian archives don't have current version of protobuf-c, so build it.
# Then build the output plugin,
# Then get rid of all that source.
RUN \
    wget -O /src/protobuf-c.tar.gz https://github.com/protobuf-c/protobuf-c/releases/download/v1.2.1/protobuf-c-1.2.1.tar.gz \
 && (cd /src; tar xf ./protobuf-c.tar.gz) \
 && (cd /src/protobuf-c-1.2.1; ./configure --disable-protoc; make install) \
 && (cd /src/pgoutput; make clean install) \
 && rm -rf /src

# Remove the build systems to reduce security surface area
RUN apt-get purge -y --auto-remove  gcc make postgresql-server-dev-9.5

# Add VOLUMEs to allow backup of config, logs and databases
VOLUME  ["/var/log/postgresql", "/var/lib/postgresql"]
