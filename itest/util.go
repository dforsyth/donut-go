package test

import (
	// Wait for etcd client v3.4, there is a module import issue.
	"github.com/coreos/etcd/clientv3" // "go.etcd.io/etcd/clientv3"
	"github.com/dforsyth/donut/coordinator"
	"time"
)

// NewTestClient creates an insecure "test" client, which points at
// 0.0.0.0
func NewTestClient() *clientv3.Client {
	endpoints := []string{"http://0.0.0.0:2379"}
	config := clientv3.Config{
		Endpoints: endpoints,
	}

	cli, err := clientv3.New(config)
	if err != nil {
		panic(err)
	}
	return cli
}

// NewTestCoordinator creates a "test" Coordinator with some simple
// defaults for testing.
func NewTestCoordinator(cli *clientv3.Client) coordinator.Coordinator {
	kv, err := coordinator.NewEtcdCoordinator(cli,
		coordinator.WithRequestTimeout(1*time.Second),
		coordinator.WithLeaseTimeout(1*time.Second))
	if err != nil {
		panic(err)
	}
	return kv
}
