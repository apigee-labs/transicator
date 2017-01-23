# This is a Dockerfile that simply builds the plugin for compatibility
# with a specific Postgres version on CentOS 7.3. It is intended to be
# run once so that we can simply extract a properly-compiled .so for
# that platform.
# For actually running a default Postgres build containing the correct
# image, please just run "Dockerfile."

FROM centos:7.3.1611

RUN \
    mkdir /src \
 && yum -y install https://download.postgresql.org/pub/repos/yum/9.6/redhat/rhel-7-x86_64/pgdg-centos96-9.6-3.noarch.rpm \
 && yum -y install gcc make wget protobuf-devel postgresql96-server postgresql96-devel

COPY . /src/pgoutput

# Yum archives don't have current version of protobuf-c, so build it.
RUN \
    wget -O /src/protobuf-c.tar.gz https://github.com/protobuf-c/protobuf-c/releases/download/v1.2.1/protobuf-c-1.2.1.tar.gz \
 && (cd /src; tar xf ./protobuf-c.tar.gz) \
 && (cd /src/protobuf-c-1.2.1; ./configure --disable-protoc; make install) \
 && (cd /src/pgoutput; PG_CONFIG=/usr/pgsql-9.6/bin/pg_config make clean all) \
 && mkdir /output \
 && cp /src/pgoutput/transicator_output.so /output \
 && cp /usr/local/lib/libprotobuf-c.so.1.0.0 /output
