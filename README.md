Donut is a library for building clustered services in Go.  It was originally modeled after Ordasity (http://github.com/boundary/ordasity).

## Example Service

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

	func (*ExampleListener) StartTask(taskId string) {}

	func (*ExampleListener) EndTask(taskId string) {}

	func (*ExampleListener) OnLeave() {}

	func main() {
		config := donut.NewConfig()
		config.Servers = []string{"localhost:2181"}
		config.NodeId = "node"
		c := donut.NewCluster("example", config, &donut.DumbBalancer{}, &ExampleListener{})
		c.Join()
		select {}
	}

## Documentation

http://go.pkgdoc.org/github.com/dforsyth/donut
