ALLDEPS = \
 *.go

local: $(ALLDEPS)
	go build -o changeserver ./

docker:
	./extract_binary.sh

localtest: local
	go test -v `glide nv`

clean:
	rm -f ./changeserver

