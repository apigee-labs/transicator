ALLDEPS = \
 *.go

all: snapshotserver

snapshotserver: $(ALLDEPS)
	go build -o $@ ./

test: snapshotserver
	go test -v `glide nv`

clean:
	rm -f ./snapshotserver

