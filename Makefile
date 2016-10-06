all: ./bin/changeserver ./bin/snapshotserver

./bin/changeserver: ./bin ./*/*.go
	go build -o $@ ./changeserver

./bin/snapshotserver: ./bin ./*/*.go
	go build -o $@ ./snapshotserver

./bin:
	mkdir bin

./test-reports:
	mkdir test-reports

tests: ./test-reports
	go test ./replication ./common ./storage ./pgclient ./snapshotserver ./changeserver

dockerTests:
	./test/dockertest.sh

clean:
	rm -f bin/changeserver
	rm -f bin/snapshotserver

docker:
	make -C ./changeserver docker
	make -C ./snapshotserver docker
	./buildpostgresdocker.sh
