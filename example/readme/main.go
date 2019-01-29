package main

import (
	"context"
	"log"
	"os"

	// Wait for etcd client v3.4, there is a module import issue.
	client "github.com/coreos/etcd/clientv3" // "go.etcd.io/etcd/clientv3"
	"github.com/dforsyth/donut/cluster"
	"github.com/dforsyth/donut/coordinator"
)

type ExampleListener struct {
	logger *log.Logger
}

func (l *ExampleListener) OnJoin(c *cluster.Cluster) {
	l.logger.Println("Joined the cluster!")
}

func (l *ExampleListener) StartWork(ctx context.Context, workKey string) {
	l.logger.Println("Starting work " + workKey)
}

func (*ExampleListener) OnLeave() {}

func main() {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	c := cluster.New("example", "node", &ExampleListener{logger: logger})
	client, err := client.New(client.Config{
		Endpoints: []string{"http://0.0.0.0:2379"},
	})
	kv, err := coordinator.NewEtcdCoordinator(client)
	if err != nil {
		logger.Fatalf("Failed to create kv: %s", err)
	}
	if err := c.Join(kv); err != nil {
		logger.Fatalf("Failed to join cluster: %s", err)
	}
	select {}
}
