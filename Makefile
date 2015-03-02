PACKAGES := \
	github.com/atulmirajkar/RPC-golang/rpcserver \
	github.com/atulmirajkar/RPC-golang/rpcclient
DEPENDENCIES := github.com/boltdb/bolt 

all: install

install: deps build
	go install testserver/testserver.go
	go install testclient/testclient.go 
build:
	go get -d github.com/atulmirajkar/RPC-golang

format:
	go fmt $(PACKAGES)

deps:
	go get $(DEPENDENCIES)

clean:
	go clean -i $(PACKAGES) $(DEPENDENCIES)
