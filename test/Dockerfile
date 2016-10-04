FROM golang:1.7.1-alpine

COPY . /go/src/github.com/30x/transicator

# We need both "testing" and "community" repositories here.
RUN \
     echo "@testing http://nl.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories \
  && echo "@community http://nl.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories \
  && apk add --no-cache \
     git gcc musl-dev leveldb@testing leveldb-dev@testing glide@community


RUN (cd /go/src/github.com/30x/transicator;glide install)

WORKDIR /go/src/github.com/30x/transicator

CMD ./test/container_test_script.sh
