package donut

import (
	"gozk"
	"log"
	"path"
	"sync/atomic"
	"time"
)

const (
	NewState = iota
	ShutdownState
	DrainingState
	StartedState
)

type Config struct {
	NodeId   string
	Servers  string
	Timeout  int64
	WorkPath string
}

func NewConfig() *Config {
	return &Config{
		WorkPath: "work",
	}
}

type Cluster struct {
	clusterName                   string
	config                        *Config
	nodes, work, claimed, owned   *SafeMap
	listener                      Listener
	balancer                      Balancer
	state                         int32
	zk                            *gozk.ZooKeeper
	zkEv                          chan gozk.Event
	nodeKill, workKill, claimKill chan byte
	// BasePath, NodePath, WorkPath, ClaimPath string
}

func NewCluster(clusterName string, config *Config, balancer Balancer, listener Listener) *Cluster {
	return &Cluster{
		clusterName: clusterName,
		config:      config,
		nodes:       NewSafeMap(nil),
		work:        NewSafeMap(nil),
		claimed:     NewSafeMap(nil),
		owned:       NewSafeMap(nil),
		listener:    listener,
		state:       NewState,
		balancer:    balancer,
	}
}

func (c *Cluster) Nodes() (_nodes []string) {
	m := c.nodes.RangeLock()
	defer c.nodes.RangeUnlock()
	for k := range m {
		_nodes = append(_nodes, k)
	}
	return
}

func (c *Cluster) Join() /* int32 */ {
	// log.Println("Join...")
	switch atomic.LoadInt32(&c.state) {
	case NewState /*, ShutdownState */ :
		zk, zkEv, err := gozk.Init(c.config.Servers, c.config.Timeout)
		if err != nil {
			panic(err)
		}
		ev := <-zkEv
		if ev.State != gozk.STATE_CONNECTED {
			panic("Failed to connect to Zookeeper")
		}
		log.Printf("Node %s connected to ZooKeeper", c.config.NodeId)
		c.zk, c.zkEv = zk, zkEv
		c.createPaths()
		c.joinCluster()
		c.listener.OnJoin(c.zk)
		c.setupWatchers()
		atomic.StoreInt32(&c.state, StartedState)
		c.getWork()
	case StartedState, DrainingState:
		log.Fatalf("Tried to join with state StartedState or DrainingState")
	default:
		panic("Unknown state")
	}
	// return atomic.LoadInt32(&c.state)
}

func (c *Cluster) createPaths() {
	base := path.Join("/", c.clusterName)
	c.zk.Create(base, "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(path.Join(base, "nodes"), "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(path.Join(base, c.config.WorkPath), "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(path.Join(base, "claim"), "", 0, gozk.WorldACL(gozk.PERM_ALL))
	log.Println("Coordination paths created")
}

func (c *Cluster) joinCluster() {
	var err error
	path := path.Join("/", c.clusterName, "nodes", c.config.NodeId)
	for {
		// log.Printf("path is %s", path)
		// XXX I should look at storing c.state in this path, for monitoring
		if _, err = c.zk.Create(path, "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err == nil {
			return
		}
		log.Printf("Attempt to join cluster failed: %v", err)
		time.Sleep(time.Second)
	}
}

func (c *Cluster) setupWatchers() (err error) {
	base := path.Join("/", c.clusterName)
	// XXX do zk.Close() here to clean up the watchers
	if c.nodeKill, err = watchZKChildren(c.zk, path.Join(base, "nodes"), c.nodes, func(m *SafeMap) {
		log.Printf("nodes updated: %s", c.nodes.Dump())
		c.getWork()
		c.verifyWork()
	}); err != nil {
		log.Printf("error setting up nodes watcher: %v", err)
		return
	}
	log.Printf("out of node watch")
	if c.workKill, err = watchZKChildren(c.zk, path.Join(base, c.config.WorkPath), c.work, func(m *SafeMap) {
		log.Printf("work updated: %s", c.work.Dump())
		c.getWork()
		c.verifyWork()
	}); err != nil {
		log.Printf("error setting up work watcher: %v", err)
		c.zk.Close()
		return
	}
	if c.claimKill, err = watchZKChildren(c.zk, path.Join(base, "claim"), c.claimed, func(m *SafeMap) {
		log.Printf("claim updated: %s", c.claimed.Dump())
		c.getWork()
		c.verifyWork()
	}); err != nil {
		log.Printf("error setting up work watcher: %v", err)
		c.zk.Close()
		return
	}
	log.Println("Watching coordination paths")
	return
}

func (c *Cluster) getWork() {
	if atomic.LoadInt32(&c.state) != StartedState {
		return
	}

	base := path.Join("/", c.clusterName, c.config.WorkPath)
	if c.work.Len() == 0 {
		log.Println("no work to be had")
		return
	}

	m := c.work.RangeLock()
	// copy the claimed map so we can use it without blocking the claim
	// update we'll see if we are successful in claiming any work.
	claimed := c.claimed.RangeLock()
	n := make(map[string]interface{})
	for k, v := range claimed {
		n[k] = v
	}
	c.claimed.RangeUnlock()
	for work := range m {
		if _, ok := n[work]; ok {
			continue
		}
		data, err := getDeserialize(c.zk, path.Join(base, work))
		if err != nil {
			// log.Printf("failed to get work: %v", err)
			continue
		}
		if c.balancer.CanClaim() {
			c.tryClaimWork(work, data)
		}
	}
	c.work.RangeUnlock()
}

func (c *Cluster) tryClaimWork(workId string, data map[string]interface{}) {
	if c.owned.Get(workId) != nil {
		return
	}

	if nodeId := c.workAssigned(workId); nodeId == "" || nodeId == c.config.NodeId {
		c.claimWork(workId, data)
	}
}

func (c *Cluster) claimWork(workId string, data map[string]interface{}) (err error) {
	claim := path.Join("/", c.clusterName, "claim", workId)
	if _, err := c.zk.Create(claim, c.config.NodeId, gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err == nil {
		c.balancer.AddWork(workId)
		c.startWork(workId, data)
	} else {
		log.Printf("Could not claim %s: %v", workId, err)
	}
	return
}

func (c *Cluster) claimAssigned(workId string, data map[string]interface{}) {
	// clusterName -> nodeId means this work is assigned to this node
	if node, ok := data[c.clusterName].(string); !ok || node != c.config.NodeId {
		return
	}
	for {
		if c.owned.Get(workId) != nil {
			// We already own the node
			return
		}
		if err := c.claimWork(workId, data); err != nil {
			time.Sleep(time.Second)
		}
	}
}

func (c *Cluster) workOwner(workId string) (node string, err error) {
	claim := path.Join("/", c.clusterName, "claim", workId)
	node, _, err = c.zk.Get(claim)
	return
}

func (c *Cluster) workAssigned(workId string) string {
	path := path.Join("/", c.clusterName, c.config.WorkPath, workId)
	if data, err := getDeserialize(c.zk, path); err == nil {
		if node, ok := data[c.clusterName].(string); ok {
			return node
		}
	}
	return ""
}

func (c *Cluster) startWork(workId string, data map[string]interface{}) {
	c.owned.Put(workId, data)
	// start listener work in a goroutine
	/// TODO provide a way to kill working goroutines (pass a kill channel or something)
	log.Printf("Starting work %s", workId)
	go c.listener.StartWork(workId, data)
}

func (c *Cluster) endWork(workId string) {
	if c.owned.Get(workId) != nil {
		// XXX Kill running routine here
		if err := c.zk.Delete(path.Join("/", c.clusterName, "claim", workId), -1); err != nil {
			log.Printf("Could not release %s: %v", workId, err)
			return
		}
		c.balancer.RemoveWork(workId)
		c.owned.Delete(workId)
	}
	c.listener.EndWork(workId)
	log.Printf("Ended work on %s", workId)
}

func (c *Cluster) CreateWork(workId string, data map[string]interface{}) (err error) {
	if err = serializeCreate(c.zk, path.Join("/", c.clusterName, c.config.WorkPath, workId), data); err != nil {
		log.Printf("Failed to create work %s: %v", workId, err)
	}
	return
}

func (c *Cluster) CompleteWork(workId string) {
	c.zk.Delete(path.Join("/", c.clusterName, c.config.WorkPath, workId), -1)
}

func (c *Cluster) verifyWork() {
	var toRelease []string
	m := c.owned.RangeLock()
	for workId := range m {
		// XXX have to use Contains() here because the watch function inserts nil values on start and we might not get an update
		if !c.work.Contains(workId) {
			log.Printf("%s is no longer in work: %s", workId, c.work.Dump())
			toRelease = append(toRelease, workId)
			continue
		}
		// fetch the workId info from zk again here because we might be stale
		if nodeId := c.workAssigned(workId); nodeId != "" && nodeId != c.config.NodeId {
			// log.Printf("%s is not assigned to this node", workId)
			// XXX drop this work
			toRelease = append(toRelease, workId)
			continue
		}
		if nodeId, _, err := c.zk.Get(path.Join("/", c.clusterName, "claim", workId)); err == nil && nodeId != c.config.NodeId {
			// log.Printf("we are doing work we shouldn't be for %s", workId)
			toRelease = append(toRelease, workId)
			continue
		}
	}
	c.owned.RangeUnlock()
	for _, workId := range toRelease {
		c.endWork(workId)
	}
}

func (c *Cluster) Shutdown() {
	log.Printf("%s shutting down", c.config.NodeId)
	atomic.StoreInt32(&c.state, ShutdownState)
	m := c.owned.RangeLock()
	c.owned.RangeUnlock()
	for workId := range m {
		c.endWork(workId)
	}
	c.finish()
	c.listener.OnLeave()
	atomic.StoreInt32(&c.state, NewState)
}

func (c *Cluster) finish() {
	// XXX Close() will clean up all the watchers
	c.zk.Close()
}

func (c *Cluster) rebalance() {
	if atomic.LoadInt32(&c.state) == NewState {
		return
	}
	for !c.balancer.CanClaim() {
		// XXX need handoff logic for this, so that work is still being done until it is reassigned
		break
	}
}
