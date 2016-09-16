all: ./bin/changeserver ./bin/snapshotserver

./bin/changeserver: ./bin ./*/*.go
	go build -o $@ ./changeserver

./bin/snapshotserver: ./bin ./*/*.go
	go build -o $@ ./snapshot

./bin:
	mkdir bin

test:
	go test `glide nv`

clean:
	rm -f bin/changeserver

