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

func (*DumbBalancer) Init(l donut.Listener) {

}

func (*DumbBalancer) AddWork(id string) {

}

func (*DumbBalancer) RemoveWork(id string) {

}

func (*DumbBalancer) CanClaim() bool {
	return true
}

func (*DumbBalancer) HandoffList() []string {
	return make([]string, 0)
}

type ExampleListener struct {
	c       *donut.Cluster
	nodeId  string
	killers map[string]chan byte
	config  *donut.Config
	dings   int
	jobs    int
}

func (l *ExampleListener) OnJoin(zk *gozk.ZooKeeper) {
	log.Println("Joining!")
	// Create some assigned work for this node as soon as it joins...
	data := make(map[string]interface{})
	// assign this work specifically to this node
	data["example"] = l.nodeId
	donut.CreateWork("example", zk, l.config, "work-"+l.nodeId, data)
	go func() {
		// only do this work for 5 seconds
		time.Sleep(20 * time.Second)
		donut.CompleteWork("example", zk, l.config, "work-"+l.nodeId)
		l.jobs--
	}()
}

func (l *ExampleListener) OnLeave() {
	log.Println("Leaving!")
}

func (l *ExampleListener) StartWork(workId string, data map[string]interface{}) {
	log.Printf("Starting work on %s!", workId)
	l.killers[workId] = make(chan byte)
	l.jobs++
	for {
		select {
		case <-l.killers[workId]:
			log.Printf("%s killed!", workId)
			return
		default:
		}
		log.Printf("ding %s!", workId)
		l.dings++
		time.Sleep(time.Second)
	}
}

func (l *ExampleListener) EndWork(workId string) {
	log.Printf("Ending work on %s!", workId)
	l.killers[workId] <- 0
}

func (l *ExampleListener) Information() map[string]interface{} {
	information := make(map[string]interface{})
	information["node_id"] = l.config.NodeId
	information["completed_iterations"] = l.dings
	information["jobs"] = l.jobs
	return information
}

func (l *ExampleListener) APIHost() string {
	return ""
}

func (l *ExampleListener) APIPort() string {
	return "8000"
}

func main() {
	if len(os.Args) == 1 {
		panic("no name arguments")
	}
	node := "node"
	for _, arg := range os.Args[1:] {
		node += "-" + arg
	}
	listener := &ExampleListener{
		killers: make(map[string]chan byte),
		nodeId:  node,
	}
	config := donut.NewConfig()

	config.Servers = "localhost:50000"
	config.NodeId = node
	log.Printf("node id is %s", node)
	config.Timeout = 1 * 1e9

	c := donut.NewCluster("example", config, &DumbBalancer{}, listener)
	listener.c = c
	listener.config = config
	c.Join()
	<-make(chan byte)
}
