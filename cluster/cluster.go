package cluster

import (
	"context"
	"errors"
	"fmt"
	stdlog "log"
	"os"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3" // "go.etcd.io/etcd/clientv3"
	"github.com/dforsyth/donut/coordinator"
	"github.com/dforsyth/donut/log"
	"golang.org/x/sync/syncmap"
)

// DefaultBalancer is the default balancer, a DumbBalancer
var DefaultBalancer = &DumbBalancer{}

const (
	StateNew     = "new"
	StateJoining = "joining"
	StateJoined  = "joined"
	StateLeaving = "leaving"
	StateLeft    = "left"

	TypeNode  = "node"
	TypeWork  = "work"
	TypeClaim = "claim"

	DefaultBalancerInterval = 5 * time.Second
	DefaultDrainTimeout     = 1 * time.Minute
	DefaultEndWorkTimeout   = 5 * time.Second
)

type Cluster struct {
	kv coordinator.Coordinator

	clusterName string
	nodeID      string

	listener        Listener
	balancer        Balancer
	balanceInterval time.Duration
	logger          log.FmtLogger
	endWorkTimeout  time.Duration
	drainTimeout    time.Duration

	nodes  *coordinator.WatchMap
	work   *coordinator.WatchMap
	claims *coordinator.WatchMap
	// TODO(dforsyth): Switch back to a mutex guarded map?
	owned *syncmap.Map

	state   string
	stateLk sync.RWMutex

	balancerFire   chan struct{}
	balancerCancel context.CancelFunc
}

// ClusterOption configures a Cluster.
type ClusterOption func(*Cluster)

func WithLogger(logger log.FmtLogger) ClusterOption {
	return func(c *Cluster) { c.logger = logger }
}

func WithBalancer(balancer Balancer) ClusterOption {
	return func(c *Cluster) { c.balancer = balancer }
}

func WithBalanceInterval(balanceInterval time.Duration) ClusterOption {
	return func(c *Cluster) { c.balanceInterval = balanceInterval }
}

func WithDrainTimeout(drainTimeout time.Duration) ClusterOption {
	return func(c *Cluster) { c.drainTimeout = drainTimeout }
}

func WithEndWorkTimeout(endWorkTimeout time.Duration) ClusterOption {
	return func(c *Cluster) { c.endWorkTimeout = endWorkTimeout }
}

// New creates a new cluster to operate on.
func New(clusterName, nodeID string, listener Listener, opts ...ClusterOption) *Cluster {
	c := &Cluster{
		state:           StateNew,
		clusterName:     clusterName,
		nodeID:          nodeID,
		listener:        listener,
		balancer:        DefaultBalancer,
		balanceInterval: DefaultBalancerInterval,
		drainTimeout:    DefaultDrainTimeout,
		endWorkTimeout:  DefaultEndWorkTimeout,
		logger:          stdlog.New(os.Stderr, "", stdlog.LstdFlags),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Cluster) transitionState(from, to string) bool {
	c.stateLk.Lock()
	defer c.stateLk.Unlock()

	if c.state != from {
		return false
	}

	c.state = to
	return true
}

// Join joins a cluster, using kv for storage and coordination.
func (c *Cluster) Join(kv coordinator.Coordinator) error {
	if kv == nil {
		return errors.New("kv must not be nil")
	}

	c.logger.Printf("Joining cluster %s", c.clusterName)
	if !c.transitionState(StateNew, StateJoining) {
		return StateTransitionError{
			From: StateNew,
			To:   StateJoining,
		}
	}

	c.kv = kv

	// Join the cluster.
	if err := c.joinCluster(); err != nil {
		return err
	}

	c.listener.OnJoin(c)

	// Start our balancer. Run this before setting up watchers so we don't
	// race on balancerFire
	c.balancerFire, c.balancerCancel = c.startBalancer()

	// Start watchers.
	if err := c.ensureWatchers(); err != nil {
		return err
	}

	c.owned = &syncmap.Map{}

	// Set state to joined, allowing this node to accept work.
	if !c.transitionState(StateJoining, StateJoined) {
		return StateTransitionError{
			From: StateJoining,
			To:   StateJoined,
		}
	}

	return nil
}

// Leave leaves the cluster.
func (c *Cluster) Leave() error {
	if !c.transitionState(StateJoined, StateLeaving) {
		return StateTransitionError{
			From: StateJoined,
			To:   StateLeaving,
		}
	}

	// Cancel our watchers
	c.work.Cancel()
	c.claims.Cancel()
	c.nodes.Cancel()

	// Cancel the balancer
	c.balancerCancel()

	// end all work
	c.drainWork()

	if err := c.kv.Delete(Key(c, TypeNode, c.nodeID)); err != nil {
		c.logger.Printf("Failed to remove node entry for %s", c.nodeID)
	}

	if !c.transitionState(StateLeaving, StateLeft) {
		return StateTransitionError{
			From: StateLeaving,
			To:   StateLeft,
		}
	}

	c.listener.OnLeave()

	c.kv = nil

	return nil
}

func (c *Cluster) ensureWatchers() error {
	nodes, err := c.kv.Watch(Key(c, TypeNode, ""), c.onChanged)
	if err != nil {
		return err
	}
	c.nodes = nodes

	work, err := c.kv.Watch(Key(c, TypeWork, ""), c.onChanged)
	if err != nil {
		return err
	}
	c.work = work

	claims, err := c.kv.Watch(Key(c, TypeClaim, ""), c.onChanged)
	if err != nil {
		return err
	}
	c.claims = claims

	return nil
}

func (c *Cluster) onChanged(m *coordinator.WatchMap) {
	select {
	case c.balancerFire <- struct{}{}:
		c.logger.Printf("Submitted to balancer")
	default:
		c.logger.Printf("Can't submit to balancer")
	}
}

func (c *Cluster) isJoined() bool {
	c.stateLk.RLock()
	defer c.stateLk.RUnlock()
	return c.state == StateJoined
}

func (c *Cluster) isLeaving() bool {
	c.stateLk.RLock()
	defer c.stateLk.RUnlock()
	return c.state == StateLeaving
}

func (c *Cluster) GetNodeID() string {
	return c.nodeID
}

func (c *Cluster) GetNodes() *coordinator.WatchMap {
	return c.nodes
}

func (c *Cluster) GetWork() *coordinator.WatchMap {
	return c.work
}

func (c *Cluster) GetClaims() *coordinator.WatchMap {
	return c.claims
}

func (c *Cluster) GetOwned() *syncmap.Map {
	return c.owned
}
func (c *Cluster) joinCluster() error {
	c.logger.Printf("Creating cluster member node")
	// Create our membership entry.
	key := Key(c, TypeNode, c.nodeID)
	if err := c.kv.StoreEphemeral(key, ""); err != nil {
		return fmt.Errorf("joinCluster::StoreEphemeral(%s): %s", key, err)
	}

	c.logger.Printf("Created cluster member node")

	return nil
}

func (c *Cluster) startBalancer() (chan struct{}, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.TODO())
	fire := make(chan struct{}, 1)
	go func() {
		for {
			c.balance()
			select {
			case <-fire:
			case <-time.After(c.balanceInterval):
			case <-ctx.Done():
				return
			}
		}
		close(fire)
	}()
	return fire, cancel
}

func (c *Cluster) balance() {
	if !c.isJoined() {
		c.logger.Printf("Cannot balance: node state is not joined")
		return
	}

	c.logger.Printf("Balancing...")

	c.getWork()
	c.verifyWork()

	c.balancer.OnBalance()
}

func (c *Cluster) getWork() {
	if !c.isJoined() {
		return
	}

	keys := c.work.Keys()
	for _, workKey := range keys {
		if _, ok := c.owned.Load(workKey); ok {
			continue
		}
		if _, ok := c.claims.Get(workKey); ok {
			continue
		}

		if c.balancer.CanClaim(workKey) {
			c.tryClaimWork(workKey)
		}
	}
}

// Claim work if it is not already owned by another node
func (c *Cluster) tryClaimWork(workKey string) {
	// Build our claims key
	claimKey := Key(c, TypeClaim, workKey)
	c.logger.Printf("Attempting to claim %s", workKey)
	if err := c.kv.StoreEphemeral(claimKey, c.nodeID); err != nil {
		c.logger.Printf("Failed to claim %s with %s: %s", workKey, claimKey, err)
		return
	}

	// Once claimed, we can start our work.
	c.startWork(workKey)
}

func verifyOwner(claimKey, ownerID string) clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(claimKey), "=", ownerID)
}

// verifyWork ensures that this instance is working on valid work. If an
// owned work is no longer found in the work cache, we immediately release
// it.
func (c *Cluster) verifyWork() {
	if !c.isJoined() {
		return
	}

	var toEnd []string
	c.owned.Range(func(_key, _ interface{}) bool {
		key := _key.(string)
		if _, ok := c.work.Get(key); !ok {
			toEnd = append(toEnd, key)
		}
		// TODO(dforsyth): Verify claims, if they do not exits kill the work
		// immediately.
		return true
	})
	// TODO(dforsyth): Verify that we are working on all the claims we say we
	// are, or nuke the claims that we don't seem to have in owned.

	// We no longer support handoffs because the implemention should be a
	// detail of the application.

	for _, key := range toEnd {
		c.logger.Printf("Ending %s", key)
		c.endWork(key)
	}
}

type work struct {
	cancel context.CancelFunc
	// Watch to find out if work is finished. TODO(dforsyth): lazy create?
	finished chan struct{}
}

func (w *work) waitForFinish(timeout time.Duration) {
	select {
	case <-w.finished:
	case <-time.After(timeout):
	}
}

func (c *Cluster) startWork(workKey string) {
	ctx, cancel := context.WithCancel(context.TODO())
	c.logger.Printf("Starting work, storing in %s", workKey)
	work := &work{
		cancel:   cancel,
		finished: make(chan struct{}),
	}
	c.owned.Store(workKey, work)

	go func() {
		c.listener.StartWork(ctx, workKey)
		close(work.finished)
	}()
}

// EndWork will attempt to have the current node to drop its claim on a given
// workKey.
func (c *Cluster) EndWork(workKey string) error {
	return c.endWork(workKey)
}

func (c *Cluster) endWork(workKey string) error {
	if c.owned == nil {
		panic("c owned is nil")
	}
	loaded, ok := c.owned.Load(workKey)
	if !ok {
		// Log that we no longer own the work
		c.logger.Printf("Wanted to cancel %s, but it is not owned", workKey)
		return NotOwnedError{WorkKey: workKey}
	}

	work := loaded.(*work)

	// Signal a cancelation
	work.cancel()
	// Wait for the work to finish
	work.waitForFinish(c.endWorkTimeout)

	// Remove the claim to this work
	claimKey := Key(c, TypeClaim, workKey)
	c.logger.Printf("Deleting key: %s", claimKey)
	// Delete the claim if we still own it.
	if err := c.kv.Delete(claimKey, verifyOwner(claimKey, c.nodeID)); err != nil {
		// Failure to remove node (or some other problem). For now we can bail
		// here and hope to remove the work from owned later.
		c.logger.Printf("Failed to remove claim node %s", err)
		return err
	}

	c.logger.Printf("Removing %s from owned", workKey)
	c.owned.Delete(workKey)

	return nil
}

func (c *Cluster) drainWork() {
	if !c.isLeaving() {
		c.logger.Printf("Cannot drain when not leaving")
		return
	}

	c.owned.Range(func(key, _ interface{}) bool {
		c.endWork(key.(string))
		return true
	})

	// Wait until owned map is empty or we timeout
	start := time.Now()
	for time.Since(start) < c.drainTimeout {
		cnt := 0
		c.owned.Range(func(_, _ interface{}) bool {
			cnt++
			return true
		})
		if cnt == 0 {
			break
		}
	}

	c.logger.Printf("All work ended")
}

// Key will create a key of any type for a given cluster. Leave ID blank to get a
// suitable prefix (to watch, for instance).
func Key(c *Cluster, t string, id string) string {
	return fmt.Sprintf("%s-%s-%s", c.clusterName, t, id)
}
