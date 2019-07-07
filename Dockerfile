FROM golang:1.12

WORKDIR /overmoon
COPY src /overmoon/src
COPY go.mod go.sum /overmoon/

RUN go build src/bin/proxy/proxy.go

VOLUME /overmoon/config.yaml
