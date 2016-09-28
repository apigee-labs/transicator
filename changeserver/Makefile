ALLDEPS = \
 *.go

local: $(ALLDEPS)
	go build -o changeserver ./

docker:
	./extract_binary.sh
	./local_docker.sh
	rm -f ./changeserver

localtest: local
	go test -v `glide nv`

clean:
	rm -f ./changeserver
