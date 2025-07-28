FROM golang

RUN apt-get update && apt-get install -y curl ca-certificates

RUN mkdir -p /etc/apt/keyrings && curl -so /etc/apt/keyrings/ceph.asc https://download.ceph.com/keys/release.asc

RUN echo "deb [signed-by=/etc/apt/keyrings/ceph.asc] https://download.ceph.com/debian-19.2.2 bookworm main" > /etc/apt/sources.list.d/ceph.list

RUN apt-get update && apt-get install -y --no-install-recommends libcephfs-dev librbd-dev librados-dev ceph-fuse ceph-common

RUN apt-get install -y iputils-ping iproute2 vim bash-completion vim colormake

WORKDIR /app
VOLUME [ "/app" ]

ENTRYPOINT [ "/bin/bash" ]