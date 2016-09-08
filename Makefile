all: ./bin/changeserver

./bin/changeserver: ./bin ./*/*.go
	go build -o $@ ./changeserver

./bin:
	mkdir bin

test:
	go test `glide nv`

clean:
	rm -f bin/changeserver

