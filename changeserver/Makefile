ALLDEPS = \
 *.go

all: changeserver

changeserver: $(ALLDEPS)
	go build -o $@ ./

test: changeserver
	go test -v `glide nv`

clean:
	rm -f ./changeserver

