package donut

import (
	"errors"
	"launchpad.net/gozk/zookeeper"
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
	NodeId            string
	Servers           string
	Timeout           time.Duration
	TasksPath         string
	HandoffPrefix     string
	RebalanceInterval time.Duration
}

func NewConfig() *Config {
	return &Config{
		TasksPath:         "tasks",
		HandoffPrefix:     "handoff",
		RebalanceInterval: time.Second * 5,
		Timeout:           time.Second,
	}
}

type Cluster struct {
	clusterName        string
	config             *Config
	nodes              *ZKMap
	tasks              *ZKMap
	claimed            *ZKMap
	owned              *SafeMap
	handoffRequest     *ZKMap
	handoffClaim       *ZKMap
	claimedHandoff     *SafeMap
	listener           Listener
	balancer           Balancer
	state              int32
	zk                 *zookeeper.Conn
	zkSession          <-chan zookeeper.Event
	basePath           string
	handoffRequestPath string
	handoffClaimPath   string
	// BasePath, NodePath, TasksPath, ClaimPath string
	rebalanceKill chan byte
}

func NewCluster(clusterName string, config *Config, balancer Balancer, listener Listener) *Cluster {
	return &Cluster{
		clusterName:    clusterName,
		config:         config,
		owned:          NewSafeMap(nil),
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
	return c.nodes._m.Keys()
}

func (c *Cluster) Tasks() []string {
	return c.tasks._m.Keys()
}

func (c *Cluster) Claimed() []string {
	return c.claimed._m.Keys()
}

func (c *Cluster) Owned() []string {
	return c.owned.Keys()
}

func (c *Cluster) ZKClient() *zookeeper.Conn {
	return c.zk
}

// Join joins the cluster *c is configured for.
func (c *Cluster) Join() error {
	// log.Println("Join...")
	switch atomic.LoadInt32(&c.state) {
	case NewState /*, ShutdownState */ :
		zk, session, err := zookeeper.Dial(c.config.Servers, c.config.Timeout)
		if err != nil {
			return err
		}
		ev := <-session
		if ev.State != zookeeper.STATE_CONNECTED {
			return errors.New("Failed to connect to Zookeeper")
		}
		log.Printf("Node %s connected to ZooKeeper", c.config.NodeId)
		c.zk, c.zkSession = zk, session
		c.createPaths()
		c.joinCluster()
		c.listener.OnJoin(zk)
		c.setupWatchers()
		if !atomic.CompareAndSwapInt32(&c.state, NewState, StartedState) {
			log.Fatalf("Could not move from NewState to StartedState: State is not NewState")
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
	c.getTasks()
	return nil
}

func (c *Cluster) createPaths() {
	c.basePath = path.Join("/", c.clusterName)
	c.zk.Create(c.basePath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	c.zk.Create(path.Join(c.basePath, "nodes"), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	c.zk.Create(path.Join(c.basePath, c.config.TasksPath), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	c.zk.Create(path.Join(c.basePath, "claim"), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	c.handoffRequestPath = c.config.HandoffPrefix + "-attempt"
	c.handoffClaimPath = c.config.HandoffPrefix + "-claim"
	c.zk.Create(path.Join(c.basePath, c.handoffRequestPath), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	c.zk.Create(path.Join(c.basePath, c.handoffClaimPath), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	log.Println("Coordination paths created")
}

func (c *Cluster) joinCluster() {
	var err error
	path := path.Join("/", c.clusterName, "nodes", c.config.NodeId)
	for {
		if _, err = c.zk.Create(path, "", zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL)); err == nil {
			return
		}
		log.Printf("Attempt to join cluster failed: %v", err)
		time.Sleep(time.Second)
	}
}

func (c *Cluster) setupWatchers() (err error) {
	base := path.Join("/", c.clusterName)

	if c.nodes, err = NewZKMap(c.zk, path.Join(base, "nodes"), func(m *ZKMap) {
		c.getTasks()
		c.verifyTasks()
	}); err != nil {
		panic(err)
	}

	if c.tasks, err = NewZKMap(c.zk, path.Join(base, c.config.TasksPath), func(m *ZKMap) {
		c.getTasks()
		c.verifyTasks()
	}); err != nil {
		panic(err)
	}

	if c.claimed, err = NewZKMap(c.zk, path.Join(base, "claim"), func(m *ZKMap) {
		c.getTasks()
		c.verifyTasks()
	}); err != nil {
		panic(err)
	}

	if c.handoffRequest, err = NewZKMap(c.zk, path.Join(base, c.handoffRequestPath), func(m *ZKMap) {
		c.getTasks()
		c.verifyTasks()
	}); err != nil {
		panic(err)
	}

	if c.handoffClaim, err = NewZKMap(c.zk, path.Join(base, c.handoffClaimPath), func(m *ZKMap) {
		c.getTasks()
		c.verifyTasks()
	}); err != nil {
		panic(err)
	}

	log.Println("Watching coordination paths")
	return
}

func (c *Cluster) getTasks() {
	if atomic.LoadInt32(&c.state) != StartedState {
		return
	}

	base := path.Join("/", c.clusterName, c.config.TasksPath)
	if c.tasks._m.Len() == 0 {
		return
	}

	m := c.tasks._m.RangeLock()
	defer c.tasks._m.RangeUnlock()
	for task := range m {
		if c.claimed._m.Contains(task) && !c.handoffRequest._m.Contains(task) {
			continue
		}
		data, err := getDeserialize(c.zk, path.Join(base, task))
		if err != nil {
			continue
		}
		if c.balancer.CanClaim() {
			c.tryClaimTask(task, data)
		}
	}
}

func (c *Cluster) tryClaimTask(taskId string, data map[string]interface{}) {
	if c.owned.Get(taskId) != nil {
		return
	}

	if nodeId := c.taskAssigned(taskId); nodeId == "" || nodeId == c.config.NodeId {
		c.claimTask(taskId, data, c.handoffRequest._m.Contains(taskId))
	}
}

func (c *Cluster) claimTask(taskId string, data map[string]interface{}, handoffClaim bool) (err error) {
	var claim string
	if handoffClaim {
		claim = path.Join(c.basePath, c.handoffClaimPath, taskId)
	} else {
		claim = path.Join("/", c.clusterName, "claim", taskId)
	}
	if _, err := c.zk.Create(claim, c.config.NodeId, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL)); err == nil {
		log.Printf("Claimed %s with %s", taskId, claim)
		if handoffClaim {
			c.claimedHandoff.Put(taskId, nil)
		}
		c.startTask(taskId, data)
	} else {
		log.Printf("Could not claim %s with %s: %v", taskId, claim, err)
	}
	return
}

func (c *Cluster) claimAssigned(taskId string, data map[string]interface{}) {
	// clusterName -> nodeId means this task is assigned to this node
	if node, ok := data[c.clusterName].(string); !ok || node != c.config.NodeId {
		return
	}
	for {
		if c.owned.Get(taskId) != nil {
			// We already own the node
			return
		}
		if err := c.claimTask(taskId, data, false); err != nil {
			log.Printf("failed to claim assigned task %s, will retry", taskId)
			time.Sleep(time.Second)
		}
	}
}

func (c *Cluster) taskOwner(taskId string) (node string, err error) {
	claim := path.Join("/", c.clusterName, "claim", taskId)
	node, _, err = c.zk.Get(claim)
	return
}

func (c *Cluster) ownTask(taskId string) bool {
	if node, err := c.taskOwner(taskId); err == nil {
		return node == c.config.NodeId
	}
	return false
}

func (c *Cluster) taskAssigned(taskId string) string {
	path := path.Join("/", c.clusterName, c.config.TasksPath, taskId)
	if data, err := getDeserialize(c.zk, path); err == nil {
		if node, ok := data[c.clusterName].(string); ok {
			return node
		}
	}
	return ""
}

// TODO: make the assignment field a list instead of a single string, so there are multiple candidates for tasks

func (c *Cluster) startTask(taskId string, data map[string]interface{}) {
	c.owned.Put(taskId, data)
	// start listener task in a goroutine
	/// TODO provide a way to kill working goroutines (pass a kill channel or something)
	log.Printf("Starting task %s", taskId)
	go c.listener.StartTask(taskId, data)
}

func (c *Cluster) endTask(taskId string) {
	if c.owned.Get(taskId) != nil {
		// XXX Kill running routine here
		c.zk.Delete(path.Join(c.basePath, "claim", taskId), -1)
		c.owned.Delete(taskId)
	}
	c.listener.EndTask(taskId)
	log.Printf("Ended task %s", taskId)
}

func (c *Cluster) verifyTasks() {
	var toRelease []string
	m := c.owned.RangeLock()
	for taskId := range m {
		// XXX have to use Contains() here because the watch function inserts nil values on start and we might not get an update
		if !c.tasks._m.Contains(taskId) {
			log.Printf("%s is no longer in tasks: %s", taskId, c.tasks._m.Dump())
			toRelease = append(toRelease, taskId)
			continue
		}
		// fetch the taskId info from zk again here because we might be stale
		if nodeId := c.taskAssigned(taskId); nodeId != "" && nodeId != c.config.NodeId {
			log.Printf("Doing a task %s that is assigned to %s", taskId, nodeId)
			toRelease = append(toRelease, taskId)
			continue
		}
		if nodeId, _, err := c.zk.Get(path.Join("/", c.clusterName, "claim", taskId)); err == nil && nodeId != c.config.NodeId && !c.claimedHandoff.Contains(taskId) {
			log.Printf("Doing a task %s that is owned by %s", taskId, nodeId)
			toRelease = append(toRelease, taskId)
			continue
		}
	}
	c.owned.RangeUnlock()
	for _, taskId := range toRelease {
		c.endTask(taskId)
	}
}

// Shutdown ends all tasks running on *c and removes it from the cluster it is a member of
func (c *Cluster) Shutdown() {
	log.Printf("%s shutting down", c.config.NodeId)
	atomic.StoreInt32(&c.state, ShutdownState)
	m := c.owned.RangeLock()
	c.owned.RangeUnlock()
	for taskId := range m {
		c.endTask(taskId)
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
				// relinquish this task to its new owner
				c.completeHandoffGive(k)
			}
		}
	}
}

func (c *Cluster) initiateHandoff(taskId string) error {
	// add task to handoff node
	if !c.claimed._m.Contains(taskId) {
		return nil
	}

	if c.handoffRequest._m.Contains(taskId) {
		return nil
	}

	handoffPath := path.Join(c.basePath, c.handoffRequestPath, taskId)
	if _, err := c.zk.Create(handoffPath, "", zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		log.Printf("Could not create handoff request for %s: %v", taskId, err)
		return err
	}
	return nil
}

func (c *Cluster) completeHandoffReceive(taskId string) {
	// XXX claim task and remove handoff claim entry
	log.Printf("Completing handoff receive for %s", taskId)
	var err error
	for {
		claim := path.Join(c.basePath, "claim", taskId)
		if _, err = c.zk.Create(claim, c.config.NodeId, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL)); err == nil || c.ownTask(claim) {
			// we have created our claim node, we can delete the handoff claim node
			c.zk.Delete(path.Join(c.basePath, c.handoffClaimPath, taskId), -1)
			c.claimedHandoff.Delete(taskId)
			break
		}
		log.Printf("Could not complete handoff for %s: %v, will retry", taskId, err)
		time.Sleep(time.Second)
	}
	log.Printf("Done with handoff receive for %s", taskId)
}

func (c *Cluster) completeHandoffGive(taskId string) {
	log.Printf("Completing handoff give for %s", taskId)
	c.zk.Delete(path.Join(c.basePath, c.handoffRequestPath, taskId), -1)
	c.endTask(taskId)
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
		for _, taskId := range handoffList {
			log.Printf("Initiating handoff for %s", taskId)
			if err := c.initiateHandoff(taskId); err != nil {
				log.Printf("Error on initiateHandoff: %v", err)
			}
		}
	}
}

func (c *Cluster) ForceRebalance() {
	c.rebalance()
}
