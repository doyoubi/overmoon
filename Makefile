build:
	go build src/bin/proxy/proxy.go

test:
	go test ./...

listetcd:
	ETCDCTL_API=3 etcdctl get --prefix=true /

