all: cluster client donutctl example-readme

build:

test:
	go test -race -v ./...

run-etcd:
	sudo rm -rf /tmp/etcd-data.tmp && mkdir -p /tmp/etcd-data.tmp && \
	  docker rmi gcr.io/etcd-development/etcd:v3.3.11 || true && \
	  docker run \
	  -p 2379:2379 \
	  -p 2380:2380 \
	  --mount type=bind,source=/tmp/etcd-data.tmp,destination=/etcd-data \
	  gcr.io/etcd-development/etcd:v3.3.11 \
	  /usr/local/bin/etcd \
	  --name s1 \
	  --data-dir /etcd-data \
	  --listen-client-urls http://0.0.0.0:2379 \
	  --advertise-client-urls http://0.0.0.0:2379 \
	  --listen-peer-urls http://0.0.0.0:2380 \
	  --initial-advertise-peer-urls http://0.0.0.0:2380 \
	  --initial-cluster s1=http://0.0.0.0:2380 \
	  --initial-cluster-token tkn \
	  --initial-cluster-state new

build-client:
	go build github.com/dforsyth/donut/client

build-cluster:
	go build github.com/dforsyth/donut/cluster

build-donutctl:
	go build -o donutctl-bin github.com/dforsyth/donut/donutctl

build-example-readme:
	go build -o example-readme github.com/dforsyth/donut/example/readme
