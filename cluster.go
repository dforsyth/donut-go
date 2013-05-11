package donut

import (
	"errors"
	zkutil "github.com/dforsyth/sprinkles/zookeeper"
	"launchpad.net/gozk/zookeeper"
	"log"
	"path"
	"strings"
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
	Servers           []string
	Timeout           time.Duration
	RebalanceInterval time.Duration
}

func NewConfig() *Config {
	return &Config{
		RebalanceInterval: time.Second * 5,
		Timeout:           time.Second,
	}
}

type Cluster struct {
	clusterName     string
	config          *Config
	nodes           *zkutil.ZKMap
	tasks           *zkutil.ZKMap
	claimed         *zkutil.ZKMap
	owned           *SafeMap
	handoffRequests *zkutil.ZKMap
	handoffClaims   *zkutil.ZKMap
	claimedHandoff  *SafeMap
	listener        Listener
	balancer        Balancer
	state           int32
	zk              *zkutil.ZooKeeper
	zkSession       <-chan zookeeper.Event
	basePath        string
	rebalanceKill   chan byte
	/* paths */
	tasksPath           string
	nodesPath           string
	claimsPath          string
	handoffRequestsPath string
	handoffClaimsPath   string
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
	return c.nodes.Keys()
}

func (c *Cluster) Tasks() []string {
	return c.tasks.Keys()
}

func (c *Cluster) TaskData(taskId string) interface{} {
	return c.tasks.Get(taskId)
}

func (c *Cluster) Claimed() []string {
	return c.claimed.Keys()
}

func (c *Cluster) Owned() []string {
	return c.owned.Keys()
}

func (c *Cluster) ZKClient() *zkutil.ZooKeeper {
	return c.zk
}

// Join joins the cluster *c is configured for.
func (c *Cluster) Join() error {
	if c.config.NodeId == "" {
		return errors.New("config requires a NodeId")
	}
	if len(c.config.Servers) == 0 {
		return errors.New("config requires Servers")
	}

	servers := strings.Join(c.config.Servers, ",")

	// log.Println("Join...")
	switch atomic.LoadInt32(&c.state) {
	case NewState /*, ShutdownState */ :
		zk, session, err := zookeeper.Dial(servers, c.config.Timeout)
		if err != nil {
			return err
		}
		ev := <-session
		if ev.State != zookeeper.STATE_CONNECTED {
			return errors.New("Failed to connect to Zookeeper")
		}
		log.Printf("Node %s connected to ZooKeeper", c.config.NodeId)
		c.zk, c.zkSession = zkutil.NewZooKeeper(zk), session
		c.createPaths()
		c.joinCluster()
		c.listener.OnJoin(c.zk)
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
	c.nodesPath = path.Join(c.basePath, "nodes")
	c.tasksPath = path.Join(c.basePath, "tasks")
	c.claimsPath = path.Join(c.basePath, "claims")
	c.handoffRequestsPath = path.Join(c.basePath, "handoff-offers")
	c.handoffClaimsPath = path.Join(c.basePath, "handoff-claims")

	c.zk.Create(c.basePath, "")
	c.zk.Create(c.nodesPath, "")
	c.zk.Create(c.tasksPath, "")
	c.zk.Create(c.claimsPath, "")
	c.zk.Create(c.handoffRequestsPath, "")
	c.zk.Create(c.handoffClaimsPath, "")

	log.Println("Coordination paths created")
}

func (c *Cluster) joinCluster() {
	var err error
	path := path.Join("/", c.clusterName, "nodes", c.config.NodeId)
	for {
		if err = c.zk.CreateEphemeral(path, ""); err == nil {
			return
		}
		log.Printf("Attempt to join cluster failed: %v", err)
		time.Sleep(time.Second)
	}
}

func (c *Cluster) setupWatchers() (err error) {
	if c.nodes, err = c.zk.NewMap(c.nodesPath, stringDeserialize, func(m *zkutil.ZKMap) {
		c.getTasks()
		c.verifyTasks()
	}); err != nil {
		panic(err)
	}

	if c.tasks, err = c.zk.NewMap(c.tasksPath, jsonDeserialize, func(m *zkutil.ZKMap) {
		c.getTasks()
		c.verifyTasks()
	}); err != nil {
		panic(err)
	}

	if c.claimed, err = c.zk.NewMap(c.claimsPath, stringDeserialize, func(m *zkutil.ZKMap) {
		c.getTasks()
		c.verifyTasks()
	}); err != nil {
		panic(err)
	}

	if c.handoffRequests, err = c.zk.NewMap(c.handoffRequestsPath, stringDeserialize, func(m *zkutil.ZKMap) {
		c.getTasks()
		c.verifyTasks()
	}); err != nil {
		panic(err)
	}

	if c.handoffClaims, err = c.zk.NewMap(c.handoffClaimsPath, stringDeserialize, func(m *zkutil.ZKMap) {
		c.completeHandoff()
		// c.getTasks()
		c.verifyTasks()
	}); err != nil {
		panic(err)
	}

	log.Println("Watching coordination paths")
	return
}

func (c *Cluster) getTasks() {
	if atomic.LoadInt32(&c.state) != StartedState || len(c.tasks.Keys()) == 0 {
		return
	}

	m := c.tasks.RangeLock()
	defer c.tasks.RangeUnlock()
	for taskId := range m {
		// if this task is already claimed and its owner is not trying to hand it off, continue
		if c.claimed.Contains(taskId) && !c.handoffRequests.Contains(taskId) {
			continue
		}

		// if this task is assigned to this node or the balancer says this node can handle it, try and claim it
		if c.taskAssigned(taskId) == c.config.NodeId || c.balancer.CanClaim(taskId) {
			// this is a task we can claim
			c.tryClaimTask(taskId)
		}
	}
}

// try and claim taskId
func (c *Cluster) tryClaimTask(taskId string) {
	if c.owned.Get(taskId) != nil {
		return
	}

	if nodeId := c.taskAssigned(taskId); nodeId == c.config.NodeId {
		// claim a task that can only be claimed by this node
		c.claimAssigned(taskId)
	} else if nodeId == "" {
		c.claimTask(taskId, c.handoffRequests.Contains(taskId))
	}
}

// claim taskId, as a handoff is handoffClaim is true
func (c *Cluster) claimTask(taskId string, handoffClaim bool) (err error) {
	var claim string
	if handoffClaim {
		claim = path.Join(c.handoffClaimsPath, taskId)
	} else {
		claim = path.Join(c.claimsPath, taskId)
	}

	if err := c.zk.CreateEphemeral(claim, c.config.NodeId); err == nil {
		log.Printf("Claimed %s with %s", taskId, claim)
		if handoffClaim {
			c.claimedHandoff.Put(taskId, nil)
		}
		c.startTask(taskId)
	} else {
		log.Printf("Could not claim %s with %s: %v", taskId, claim, err)
	}

	return
}

// claim a task assigned to this node
func (c *Cluster) claimAssigned(taskId string) {
	if c.taskAssigned(taskId) != c.config.NodeId {
		return
	}

	for {
		if c.owned.Contains(taskId) || !c.tasks.Contains(taskId) {
			// This node already owns this task, or it's gone from the global task set
			return
		}
		if err := c.claimTask(taskId, false); err != nil {
			log.Printf("failed to claim assigned task %s, will retry", taskId)
			time.Sleep(time.Second)
		}
	}
}

// returns the owner of a task
func (c *Cluster) taskOwner(taskId string) (node string, err error) {
	if data := c.claimed.Get(taskId); data != nil {
		return data.(string), nil
	}
	return "", errors.New(taskId + " not in claimed.")
}

// determine this node owns a task
func (c *Cluster) ownTask(taskId string) bool {
	if node, err := c.taskOwner(taskId); err == nil {
		return node == c.config.NodeId
	}
	return false
}

// determine which node a task is assigned to
func (c *Cluster) taskAssigned(taskId string) string {
	info := c.TaskData(taskId)
	if info == nil {
		log.Printf("taskInfo for %s not available", taskId)
	}
	data := info.(map[string]interface{})
	if nodeId, ok := data[c.clusterName]; ok {
		return nodeId.(string)
	}
	return ""
}

// start taskId
func (c *Cluster) startTask(taskId string) {
	// TODO check to see if this put succeeds.  If it doesn't, do not start a new task (it is already started)
	c.owned.Put(taskId, nil)
	// start listener task in a goroutine
	/// TODO provide a way to kill working goroutines (pass a kill channel or something)
	log.Printf("Starting task %s", taskId)
	go c.listener.StartTask(taskId)
}

// end taskId
func (c *Cluster) endTask(taskId string) {
	if c.owned.Contains(taskId) {
		c.zk.Delete(path.Join(c.claimsPath, taskId))
		c.owned.Delete(taskId)
	}
	c.listener.EndTask(taskId)
	log.Printf("Ended task %s", taskId)
}

// verify the tasks this node it working on, releasing any that appear to be bogus
func (c *Cluster) verifyTasks() {
	var toRelease []string
	m := c.owned.RangeLock()
	for taskId := range m {
		if !c.tasks.Contains(taskId) {
			log.Printf("%s is no longer in tasks: %s", taskId, c.tasks.Dump())
			toRelease = append(toRelease, taskId)
			continue
		}
		if nodeId := c.taskAssigned(taskId); nodeId != "" && nodeId != c.config.NodeId {
			log.Printf("Doing a task %s that is assigned to %s", taskId, nodeId)
			toRelease = append(toRelease, taskId)
			continue
		}
		if nodeId, err := c.taskOwner(taskId); err == nil && nodeId != c.config.NodeId && !c.claimedHandoff.Contains(taskId) {
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
	atomic.StoreInt32(&c.state, DrainingState)
	c.rebalanceKill <- 1
	m := c.owned.RangeLock()
	c.owned.RangeUnlock()
	// TODO: drain properly instead of just assuming everything stops when endTask is called
	for taskId := range m {
		c.endTask(taskId)
	}
	atomic.StoreInt32(&c.state, ShutdownState)
	c.finish()
	c.listener.OnLeave()
	atomic.StoreInt32(&c.state, NewState)
}

func (c *Cluster) finish() {
	// XXX Close() will clean up all the watchers
	c.zk.Close()
}

func (c *Cluster) completeHandoff() {
	// XXX this is the slow way to do this, we should just get the node that changed from zookeeper
	m := c.handoffClaims.RangeLock()
	defer c.handoffClaims.RangeUnlock()
	for k := range m {
		if c.claimedHandoff.Contains(k) {
			// complete the handoff
			c.completeHandoffReceive(k)
		} else if c.owned.Contains(k) {
			if nodeId, err := c.zk.Get(path.Join(c.handoffClaimsPath, k)); err == nil && nodeId != c.config.NodeId {
				// relinquish this task to its new owner
				c.completeHandoffGive(k)
			}
		}
	}
}

func (c *Cluster) initiateHandoff(taskId string) error {
	// TODO add a timer that will drop the task from this node even if another node hasnt claimed it

	if !c.claimed.Contains(taskId) {
		return errors.New("claimed does not contain " + taskId)
	}

	if c.handoffRequests.Contains(taskId) {
		return errors.New("handoff requests already contains " + taskId)
	}

	handoffPath := path.Join(c.handoffRequestsPath, taskId)
	if err := c.zk.CreateEphemeral(handoffPath, ""); err != nil {
		log.Printf("Could not create handoff request for %s: %v", taskId, err)
		return err
	}
	return nil
}

func (c *Cluster) completeHandoffReceive(taskId string) {
	// XXX claim task and remove handoff claim entry
	log.Printf("Completing handoff receive for %s", taskId)
	var err error
	claim := path.Join(c.claimsPath, taskId)
	for {
		if err = c.zk.CreateEphemeral(claim, c.config.NodeId); err == nil || c.ownTask(claim) {
			// we have created our claim node, we can delete the handoff claim node
			c.zk.Delete(path.Join(c.handoffClaimsPath, taskId))
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
	c.zk.Delete(path.Join(c.handoffRequestsPath, taskId))
	c.endTask(taskId)
}

func (c *Cluster) rebalance() {
	log.Println("Doing rebalance")
	if atomic.LoadInt32(&c.state) == NewState {
		return
	}

	handoffList := c.balancer.HandoffList()
	// XXX There needs to be a way to end initiated handoffs that we no longer want to execute.
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
