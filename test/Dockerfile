FROM thirtyx/go-leveldb:1.7.1

COPY . /go/src/github.com/30x/transicator

RUN \
     rm -rf /go/src/github.com/30x/transicator/vendor \
  && (cd /go/src/github.com/30x/transicator;glide install)

WORKDIR /go/src/github.com/30x/transicator

CMD ./test/container_test_script.sh
