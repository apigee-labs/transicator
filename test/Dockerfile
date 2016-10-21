FROM thirtyx/go-leveldb:1.7.1

COPY . /go/src/github.com/30x/transicator

RUN \
   (cd /go/src/github.com/30x/transicator;glide install)

WORKDIR /go/src/github.com/30x/transicator
