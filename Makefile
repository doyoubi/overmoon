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

build-docker:
	docker image build -f docker/Dockerfile-builder -t overmoon_builder .
	sh docker/rebuild.sh
	docker image build -f docker/Dockerfile-overmoon -t overmoon .

rebuild-docker:
	sh docker/rebuild.sh
	docker image build -f docker/Dockerfile-overmoon -t overmoon .

.PHONY: build test lint listetcd clearetcd
