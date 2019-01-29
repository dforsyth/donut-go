#!/usr/bin/env bash

rm -rf etcd-data.tmp && mkdir -p etcd-data.tmp && \
    docker rmi -f gcr.io/etcd-development/etcd:v3.3.11 || true && \
    docker run \
    -p 2379:2379 \
    -p 2380:2380 \
    --name ${1} \
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