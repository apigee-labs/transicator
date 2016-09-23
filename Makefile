all: ./bin/changeserver ./bin/snapshotserver

./bin/changeserver: ./bin ./*/*.go
	go build -o $@ ./changeserver

./bin/snapshotserver: ./bin ./*/*.go
	go build -o $@ ./snapshotserver

./bin:
	mkdir bin

test:
	go test `glide nv`

clean:
	rm -f bin/changeserver
	rm -f bin/snapshotserver

