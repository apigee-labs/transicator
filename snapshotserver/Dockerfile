# Build and run the snapshot server.
#
# This image will launch the server with the default port (port 9000) open.
#
# To run, add the "-u" argument, which takes a postgres URL.
#
# For instance:
#
# docker run --rm -t snapshotserver -u postgres://user:pass@host/databasename?ssl=false

FROM alpine:3.4

COPY snapshotserver /
COPY metadata/apigee-file.yaml /

EXPOSE 9000

ENTRYPOINT ["/snapshotserver", "-p", "9000", "-u", "postgres://docker@10.1.15.207:5432/docker"]
