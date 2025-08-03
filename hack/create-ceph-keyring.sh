#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")

CLIENT_NAME="afero-test"

OUT_PATH="${SCRIPT_DIR}/etc-ceph/ceph.client.${CLIENT_NAME}.keyring"

kubectl rook-ceph ceph auth get-or-create client."$CLIENT_NAME" mon 'allow *' osd 'allow *' mds 'allow *'

kubectl rook-ceph ceph auth get client."$CLIENT_NAME" > "$OUT_PATH"