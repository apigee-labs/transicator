FROM golang:latest 

RUN mkdir /app
ADD loadgen/loadgen.go /app/
WORKDIR /app 
RUN go get github.com/lib/pq
RUN go build -o loadgen . 
ENTRYPOINT ["/app/loadgen"]

