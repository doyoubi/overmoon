#!/usr/bin/env bash
go build src/bin/overmoon/overmoon.go
mkdir -p /overmoon/shared
cp /overmoon/overmoon /overmoon/shared/
