ALLDEPS = \
 *.go

docker:
	./extract_binary.sh

local: $(ALLDEPS)
	go build -o snapshotserver ./

localtest: local
	go test -v `glide nv`

clean:
	rm -f ./snapshotserver

