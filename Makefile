all: ./bin/changeserver ./bin/snapshotserver

./bin/changeserver: ./bin ./*/*.go
	go build -o $@ ./changeserver

./bin/snapshotserver: ./bin ./*/*.go
	go build -o $@ ./snapshotserver

./bin:
	mkdir bin

test:
	go test ./replication ./common ./storage ./pgclient ./snapshotserver ./changeserver

clean:
	rm -f bin/changeserver
	rm -f bin/snapshotserver

docker:
	make -C ./changeserver docker
	make -C ./snapshotserver docker
	./buildpostgresdocker.sh
