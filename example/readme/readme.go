package main

import (
	"github.com/dforsyth/donut"
	"github.com/dforsyth/sprinkles/zookeeper"
	"log"
)

type ExampleListener struct{}

func (*ExampleListener) OnJoin(zk *zookeeper.ZooKeeper) {
	log.Println("Joined the cluster!")
}

func (*ExampleListener) StartTask(workId string) {}

func (*ExampleListener) EndTask(workId string) {}

func (*ExampleListener) OnLeave() {}

func main() {
	config := donut.NewConfig()
	config.Servers = []string{"localhost:2181"}
	config.NodeId = "node"
	c := donut.NewCluster("example", config, &donut.DumbBalancer{}, &ExampleListener{})
	c.Join()
	select {}
}
