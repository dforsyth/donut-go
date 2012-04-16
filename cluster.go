package donut

import (
	"encoding/json"
	"errors"
	"gozk"
	"log"
	"net"
	"net/http"
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
	NodeId            string
	Servers           string
	Timeout           int64
	WorkPath          string
	HandoffPrefix     string
	RebalanceInterval time.Duration
}

func NewConfig() *Config {
	return &Config{
		WorkPath:          "work",
		HandoffPrefix:     "handoff",
		RebalanceInterval: time.Second * 5,
	}
}

type Cluster struct {
	clusterName                                                               string
	config                                                                    *Config
	nodes, work, claimed, owned, handoffRequest, handoffClaim, claimedHandoff *SafeMap
	listener                                                                  Listener
	balancer                                                                  Balancer
	state                                                                     int32
	zk                                                                        *gozk.ZooKeeper
	zkEv                                                                      chan gozk.Event
	nodeKill, workKill, claimKill, handoffRequestKill, handoffClaimKill       chan byte
	basePath, handoffRequestPath, handoffClaimPath                            string
	// BasePath, NodePath, WorkPath, ClaimPath string
	rebalanceKill chan byte
}

func NewCluster(clusterName string, config *Config, balancer Balancer, listener Listener) *Cluster {
	return &Cluster{
		clusterName:    clusterName,
		config:         config,
		nodes:          NewSafeMap(nil),
		work:           NewSafeMap(nil),
		claimed:        NewSafeMap(nil),
		owned:          NewSafeMap(nil),
		handoffRequest: NewSafeMap(nil),
		handoffClaim:   NewSafeMap(nil),
		claimedHandoff: NewSafeMap(nil),
		listener:       listener,
		state:          NewState,
		balancer:       balancer,
	}
}

func (c *Cluster) Name() string {
	return c.clusterName
}

func (c *Cluster) Nodes() []string {
	return c.nodes.Keys()
}

func (c *Cluster) Work() []string {
	return c.work.Keys()
}

func (c *Cluster) Claimed() []string {
	return c.claimed.Keys()
}

func (c *Cluster) Owned() []string {
	return c.owned.Keys()
}

func (c *Cluster) ZKClient() *gozk.ZooKeeper {
	return c.zk
}

// Join joins the cluster *c is configured for.
func (c *Cluster) Join() error {
	// log.Println("Join...")
	switch atomic.LoadInt32(&c.state) {
	case NewState /*, ShutdownState */ :
		zk, zkEv, err := gozk.Init(c.config.Servers, c.config.Timeout)
		if err != nil {
			return err
		}
		ev := <-zkEv
		if ev.State != gozk.STATE_CONNECTED {
			return errors.New("Failed to connect to Zookeeper")
		}
		log.Printf("Node %s connected to ZooKeeper", c.config.NodeId)
		c.zk, c.zkEv = zk, zkEv
		c.createPaths()
		c.joinCluster()
		c.listener.OnJoin(zk)
		c.setupWatchers()
		if !atomic.CompareAndSwapInt32(&c.state, NewState, StartedState) {
			log.Fatalf("Could not move from NewState to StartedState: State is not NewState")
		}
		if l, ok := c.listener.(MonitoredListener); ok {
			go startHTTP(l)
		}
	case StartedState, DrainingState:
		return errors.New("Tried to join with state StartedState or DrainingState")
	case ShutdownState:
		// TODO
	default:
		panic("Unknown state")
	}

	c.balancer.Init(c)
	go func() {
		c.rebalanceKill = make(chan byte)
		for {
			select {
			case <-c.rebalanceKill:
				return
			case <-time.After(c.config.RebalanceInterval):
				c.rebalance()
			}
		}
	}()
	c.getWork()
	return nil
}

func (c *Cluster) createPaths() {
	c.basePath = path.Join("/", c.clusterName)
	c.zk.Create(c.basePath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(path.Join(c.basePath, "nodes"), "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(path.Join(c.basePath, c.config.WorkPath), "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(path.Join(c.basePath, "claim"), "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.handoffRequestPath = c.config.HandoffPrefix + "-attempt"
	c.handoffClaimPath = c.config.HandoffPrefix + "-claim"
	c.zk.Create(path.Join(c.basePath, c.handoffRequestPath), "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(path.Join(c.basePath, c.handoffClaimPath), "", 0, gozk.WorldACL(gozk.PERM_ALL))
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
		log.Printf("nodes updated:\n%s", c.nodes.Dump())
		c.getWork()
		c.verifyWork()
	}); err != nil {
		log.Printf("error setting up nodes watcher: %v", err)
		return
	}
	log.Printf("out of node watch")
	if c.workKill, err = watchZKChildren(c.zk, path.Join(base, c.config.WorkPath), c.work, func(m *SafeMap) {
		log.Printf("work updated:\n%s", c.work.Dump())
		c.getWork()
		c.verifyWork()
	}); err != nil {
		log.Printf("error setting up work watcher: %v", err)
		c.zk.Close()
		return
	}
	if c.claimKill, err = watchZKChildren(c.zk, path.Join(base, "claim"), c.claimed, func(m *SafeMap) {
		log.Printf("claim updated:\n%s", c.claimed.Dump())
		c.getWork()
		c.verifyWork()
	}); err != nil {
		log.Printf("error setting up claim watcher: %v", err)
		c.zk.Close()
		return
	}
	if c.handoffRequestKill, err = watchZKChildren(c.zk, path.Join(c.basePath, c.handoffRequestPath), c.handoffRequest, func(m *SafeMap) {
		log.Printf("handoff requests updated:\n%s", c.handoffRequest.Dump())
		c.getWork()
		c.verifyWork()
	}); err != nil {
		c.zk.Close()
	}
	if c.handoffClaimKill, err = watchZKChildren(c.zk, path.Join(c.basePath, c.handoffClaimPath), c.handoffClaim, func(m *SafeMap) {
		log.Printf("claimed requests updated:\n%s", c.handoffClaim.Dump())
		c.handleHandoff(m)
	}); err != nil {
		c.zk.Close()
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
		return
	}

	m := c.work.RangeLock()
	defer c.work.RangeUnlock()
	for work := range m {
		if c.claimed.Contains(work) && !c.handoffRequest.Contains(work) {
			continue
		}
		data, err := getDeserialize(c.zk, path.Join(base, work))
		if err != nil {
			continue
		}
		if c.balancer.CanClaim() {
			c.tryClaimWork(work, data)
		}
	}
}

func (c *Cluster) tryClaimWork(workId string, data map[string]interface{}) {
	if c.owned.Get(workId) != nil {
		return
	}

	if nodeId := c.workAssigned(workId); nodeId == "" || nodeId == c.config.NodeId {
		c.claimWork(workId, data, c.handoffRequest.Contains(workId))
	}
}

func (c *Cluster) claimWork(workId string, data map[string]interface{}, handoffClaim bool) (err error) {
	var claim string
	if handoffClaim {
		claim = path.Join(c.basePath, c.handoffClaimPath, workId)
	} else {
		claim = path.Join("/", c.clusterName, "claim", workId)
	}
	if _, err := c.zk.Create(claim, c.config.NodeId, gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err == nil {
		log.Printf("Claimed %s with %s", workId, claim)
		if handoffClaim {
			c.claimedHandoff.Put(workId, c.config.NodeId)
		}
		c.startWork(workId, data)
	} else {
		log.Printf("Could not claim %s with %s: %v", workId, claim, err)
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
		if err := c.claimWork(workId, data, false); err != nil {
			log.Printf("failed to claim assigned work %s, will retry", workId)
			time.Sleep(time.Second)
		}
	}
}

func (c *Cluster) workOwner(workId string) (node string, err error) {
	claim := path.Join("/", c.clusterName, "claim", workId)
	node, _, err = c.zk.Get(claim)
	return
}

func (c *Cluster) ownWork(workId string) bool {
	if node, err := c.workOwner(workId); err == nil {
		return node == c.config.NodeId
	}
	return false
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

// TODO: make the assignment field a list instead of a single string, so there are multiple candidates for work

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
		c.zk.Delete(path.Join(c.basePath, "claim", workId), -1)
		c.owned.Delete(workId)
	}
	c.listener.EndWork(workId)
	log.Printf("Ended work on %s", workId)
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
			log.Printf("Doing work %s that is assigned to %s", workId, nodeId)
			toRelease = append(toRelease, workId)
			continue
		}
		if nodeId, _, err := c.zk.Get(path.Join("/", c.clusterName, "claim", workId)); err == nil && nodeId != c.config.NodeId && !c.claimedHandoff.Contains(workId) {
			log.Printf("Doing work %s that is owned by %s", workId, nodeId)
			toRelease = append(toRelease, workId)
			continue
		}
	}
	c.owned.RangeUnlock()
	for _, workId := range toRelease {
		c.endWork(workId)
	}
}

// Shutdown ends all work running on *c and removes it from the cluster it is a member of
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

func (c *Cluster) handleHandoff(handoffClaims *SafeMap) {
	m := handoffClaims.RangeLock()
	defer handoffClaims.RangeUnlock()
	for k := range m {
		if c.claimedHandoff.Contains(k) {
			// complete the handoff
			c.completeHandoffReceive(k)
		} else if c.owned.Contains(k) {
			if nodeId, _, err := c.zk.Get(path.Join(c.basePath, c.handoffClaimPath, k)); err == nil && nodeId != c.config.NodeId {
				// relinquish this work to its new owner
				c.completeHandoffGive(k)
			}
		}
	}
}

func (c *Cluster) initiateHandoff(workId string) error {
	// add work to handoff node
	if !c.claimed.Contains(workId) {
		return nil
	}

	if c.handoffRequest.Contains(workId) {
		return nil
	}

	handoffPath := path.Join(c.basePath, c.handoffRequestPath, workId)
	if _, err := c.zk.Create(handoffPath, "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
		log.Printf("Could not create handoff request for %s: %v", workId, err)
		return err
	}
	return nil
}

func (c *Cluster) completeHandoffReceive(workId string) {
	// XXX claim work and remove handoff claim entry
	log.Printf("Completing handoff receive for %s", workId)
	var err error
	for {
		claim := path.Join(c.basePath, "claim", workId)
		if _, err = c.zk.Create(claim, c.config.NodeId, gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err == nil || c.ownWork(claim) {
			// we have created our claim node, we can delete the handoff claim node
			c.zk.Delete(path.Join(c.basePath, c.handoffClaimPath, workId), -1)
			c.claimedHandoff.Delete(workId)
			break
		}
		log.Printf("Could not complete handoff for %s: %v, will retry", workId, err)
		time.Sleep(time.Second)
	}
	log.Printf("Done with handoff receive for %s", workId)
}

func (c *Cluster) completeHandoffGive(workId string) {
	log.Printf("Completing handoff give for %s", workId)
	c.zk.Delete(path.Join(c.basePath, c.handoffRequestPath, workId), -1)
	c.endWork(workId)
}

func (c *Cluster) rebalance() {
	log.Println("Doing rebalance")
	if atomic.LoadInt32(&c.state) == NewState {
		return
	}

	if c.balancer.CanClaim() {
		return
	}

	handoffList := c.balancer.HandoffList()
	if len(handoffList) > 0 {
		for _, workId := range handoffList {
			log.Printf("Initiating handoff for %s", workId)
			if err := c.initiateHandoff(workId); err != nil {
				log.Printf("Error on initiateHandoff: %v", err)
			}
		}
	}
}

func (c *Cluster) ForceRebalance() {
	c.rebalance()
}

func startHTTP(l MonitoredListener) {
	http.HandleFunc("/information", func(w http.ResponseWriter, r *http.Request) {
		information := l.Information()
		enc, err := json.Marshal(information)
		if err != nil {
			log.Printf("Could not encode data for information request")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(enc)
	})
	log.Println("Starting http server")
	log.Fatal(http.ListenAndServe(net.JoinHostPort(l.APIHost(), l.APIPort()), nil))
}
