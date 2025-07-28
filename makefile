build-dev-container:
	@docker build -t afero-cephfs-dev -f hack/Dockerfile

create-ceph-configs:
	hack/create-ceph-conf.sh
	hack/create-ceph-keyring.sh

run-dev: build-dev-container
	@docker run -it --rm --name afero-cephfs-dev \
		-v ./:/app:z \
		-v ./hack/etc-ceph:/etc/ceph:z \
		-v afero-cephfs-dev_go:/go \
		-e CEPH_ARGS="-n=client.afero-test" \
		--privileged \
		afero-cephfs-dev