# Build and run the change server.
#
# This image will launch the server with the default port (port 9000) open.
#
# To run, add the -s argument and the -u argument
# The -u argument takes a Postgres URL.
# The -s argument takes the name of the logical replication slot to create
#
# Each instance of the change server must have a different logical replication
# slot (otherwise, changes will be lost). Furthermore, the logical
# replication slot lasts forever -- when instances are shut down, the slot
# must be removed, or else the database will never be able to purge
# its transaction logs.
#
# For instance:
#
# docker run --rm -t changeserver -s test -u postgres://user:pass@host/databasename?ssl=false

FROM golang:1.7.1-alpine

RUN \
     echo "@testing http://nl.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories \
  && echo "@community http://nl.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories \
  && apk add --no-cache \
     leveldb-dev@testing glide@community && mkdir /data

COPY changeserver /
COPY metadata/apigee-file.yaml /

EXPOSE 9000
VOLUME [ "/data" ]

ENTRYPOINT ["/changeserver", "-p", "9000", "-u", "postgres://docker@10.1.15.207:5432/docker", "-d", "/data"]


