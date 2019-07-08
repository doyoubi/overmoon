#!/usr/bin/env bash
go build src/bin/proxy/proxy.go
mkdir -p /overmoon/shared
cp /overmoon/proxy /overmoon/shared/
