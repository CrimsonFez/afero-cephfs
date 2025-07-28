# CephFS backend for Afero

## About
Provides an [afero filesystem](https://github.com/spf13/afero/) implementation of an [CephFS](https://docs.ceph.com/en/latest/cephfs/) backend.  
The backend is connected to with the [go-ceph/cephfs](https://github.com/ceph/go-ceph) package.

## How to use

This wrapper indirectly relies on Ceph's C code, so we will authenticate the same way.  
That means we need our ceph.conf and keyring files in /etc/ceph/. We set our client name via the `CEPH_ARGS` environment variable.  
See the `hack/` directory for a dockerfile and some scripts to run against rook-ceph.

```golang

import (
    "os"

    cephfs "github.com/crimsonfez/afero-cephfs"
)

func main() {
    mount, err := cephfs.NewCephFS()
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }
    defer mount.Unmount()

    for _, s := range mount.Readdirnames(-1) {
        fmt.Println(s)
    }
}
```

## Testing
The tests rely on a running ceph cluster. See the `hack/` dir and the makefile for scripts to connect to a cluster running via rook-ceph.