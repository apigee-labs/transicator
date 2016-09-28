ALLDEPS = \
 *.go

docker:
	./extract_binary.sh
	./local_docker.sh
	rm -f ./snapshotserver

local: $(ALLDEPS)
	go build -o snapshotserver ./

localtest: local
	go test -v `glide nv`

clean:
	rm -f ./snapshotserver
