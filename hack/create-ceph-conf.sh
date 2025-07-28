#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")

OUT_PATH="${SCRIPT_DIR}/etc-ceph/ceph.conf"

FSID=$(kubectl rook-ceph ceph fsid)
MONS=$(kubectl rook-ceph mons)

cat > "$OUT_PATH" <<EOF
[global]
fsid = "$FSID"
mon_host = "$MONS"

[client]
name = afero-test
keyring = /etc/ceph/ceph.client.afero-test.keyring

EOF