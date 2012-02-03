package main

import (
	"donut"
	"gozk"
	"log"
	"os"
	"time"
)

type DumbBalancer struct {
}

func (*DumbBalancer) Init(c *donut.Cluster) {

}

func (*DumbBalancer) AddWork(id string) {

}

func (*DumbBalancer) RemoveWork(id string) {

}

func (*DumbBalancer) CanClaim() bool {
	return true
}

type ExampleListener struct {
	c       *donut.Cluster
	nodeId  string
	killers map[string]chan byte
}

func (l *ExampleListener) OnJoin(zk *gozk.ZooKeeper) {
	log.Println("Joining!")
	// Create some assigned work for this node as soon as it joins...
	data := make(map[string]interface{})
	// assign this work specifically to this node
	data["example"] = l.nodeId
	l.c.CreateWork("work-"+l.nodeId, data)
	go func() {
		// only do this work for 10 seconds
		time.Sleep(5 * time.Second)
		l.c.CompleteWork("work-" + l.nodeId)
	}()
}

func (l *ExampleListener) OnLeave() {
	log.Println("Leaving!")
}

func (l *ExampleListener) StartWork(workId string, data map[string]interface{}) {
	log.Printf("Starting work on %s!", workId)
	l.killers[workId] = make(chan byte)
	for {
		select {
		case <-l.killers[workId]:
			log.Printf("%s killed!", workId)
			return
		default:
		}
		log.Printf("ding %s!", workId)
		time.Sleep(time.Second)
	}
}

func (l *ExampleListener) EndWork(workId string) {
	log.Printf("Ending work on %s!", workId)
	l.killers[workId] <- 0
}

func main() {
	if len(os.Args) == 1 {
		panic("no name arguments")
	}
	node := "node"
	for _, arg := range os.Args[1:] {
		node += "-" + arg
	}
	listener := &ExampleListener{killers: make(map[string]chan byte), nodeId: node}
	config := donut.NewConfig()

	config.Servers = "localhost:50000"
	config.NodeId = node
	config.Timeout = 1 * 1e9

	c := donut.NewCluster("example", config, &DumbBalancer{}, listener)
	listener.c = c
	c.Join()
	<-make(chan byte)
}
