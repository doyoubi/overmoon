build:
	go build src/bin/proxy/proxy.go

test:
	go test ./...

lint:
	go fmt $(shell go list ./...)
	golint ./src/...

listetcd:
	ETCDCTL_API=3 etcdctl get --prefix=true /

clearetcd:
	ETCDCTL_API=3 etcdctl del --prefix=true /

.PHONY: build test lint listetcd clearetcd

