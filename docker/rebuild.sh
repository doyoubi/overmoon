#!/usr/bin/env bash

TARGET_VOLUME="${PWD}/docker/shared:/overmoon/shared"
MOD_VOLUME="${PWD}/go.mod:/overmoon/go.mod"
SUM_VOLUME="${PWD}/go.sum:/overmoon/go.sum"
SRC_VOLUME="${PWD}/src:/overmoon/src"
COPY_SCRIPT_VOLUME="${PWD}/docker/copy_target.sh:/overmoon/copy_target.sh"

docker run --rm -v "${TARGET_VOLUME}" -v "${MOD_VOLUME}" -v "${SUM_VOLUME}" -v "${SRC_VOLUME}" -v "${COPY_SCRIPT_VOLUME}" overmoon_builder sh /overmoon/copy_target.sh
