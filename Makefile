SUBDIRS = ./replication ./common ./storage ./pgclient ./snapshotserver ./changeserver

%.checked: 
	(cd $*; ../presubmit_tests.sh)

all: ./bin/changeserver ./bin/snapshotserver

./bin/changeserver: ./bin ./*/*.go
	go build -o $@ ./changeserver

./bin/snapshotserver: ./bin ./*/*.go
	go build -o $@ ./snapshotserver

./bin/changeserver-rocksdb: ./bin ./*/*.go
	go build -tags rocksdb -o $@ ./changeserver

rocksdb: ./bin/changeserver-rocksdb ./bin/snapshotserver

./bin:
	mkdir bin

./test-reports:
	mkdir test-reports

tests: ./test-reports
	go test $(SUBDIRS) 

dockerTests:
	./test/dockertest.sh

presubmit: $(foreach d, $(SUBDIRS), ./$d.checked) pgoutput.checked

clean:
	rm -f bin/changeserver
	rm -f bin/snapshotserver

docker:
	docker build -f pgoutput/Dockerfile ./pgoutput/ -t apigeelabs/transicator-postgres
	docker build -f Dockerfile.changeserver . -t apigeelabs/transicator-changeserver
	docker build -f Dockerfile.snapshotserver . -t apigeelabs/transicator-snapshot
