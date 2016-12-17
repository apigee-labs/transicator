# Build and run the snapshot server.
#
# This image will launch the server with the default port (port 9000) open.
#
# To run, add the "-u" argument, which takes a postgres URL.
#
# For instance:
#
# docker run --rm -t snapshotserver -u postgres://user:pass@host/databasename?ssl=false

FROM golang:1.7.3-alpine

RUN \
  apk add --no-cache gcc linux-headers musl-dev

COPY . /go/src/github.com/apigee-labs/transicator

RUN \
    (cd /go/src/github.com/apigee-labs/transicator; go build -o /snapshotserver ./cmd/snapshotserver) \
 && mkdir /keys \
 && rm -r /go

EXPOSE 9001 9444 10001

VOLUME [ "/keys" ]

ENTRYPOINT [ "/snapshotserver", "--mgmtport", "10001"]
