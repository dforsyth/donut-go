Donut is a library for building clustered services in Go.  It was originally modeled after Ordasity (http://github.com/boundary/ordasity).

## Example Service

	package main

	import (
		"github.com/dforsyth/donut"
		"launchpad.net/gozk/zookeeper"
		"log"
	)

	type ExampleListener struct{}

	func (*ExampleListener) OnJoin(zk *zookeeper.Conn) {
		log.Println("Joined the cluster!")
	}

	func (*ExampleListener) StartWork(workId string, data map[string]interface{}) {}

	func (*ExampleListener) EndWork(workId string) {}

	func (*ExampleListener) OnLeave() {}

	func main() {
		config := donut.NewConfig()
		config.Servers = "localhost:50000"
		config.NodeId = "node"
		c := donut.NewCluster("example", config, &donut.DumbBalancer{}, &ExampleListener{})
		c.Join("")
		<-make(chan byte)
	}

## Documentation

http://go.pkgdoc.org/github.com/dforsyth/donut
