all: ./bin/changeserver

./bin/changeserver: ./bin ./changeserver/*.go
	go build -o $@ ./changeserver

./bin:
	mkdir bin

test:
	go test `glide nv`
