package donut

import (
	"sync"
	"math"
)

type Balancer interface {
	// Prepare the balancer to start balancing
	Init(c *Cluster)
	// Indicates whether the listener that this balancer is attached to can claim new work or not
	CanClaim() bool
	// Work to be released by this listener in a rebalance
	HandoffList() []string
}

type DumbBalancer struct {
}

func (*DumbBalancer) Init(c *Cluster) {

}

func (*DumbBalancer) CanClaim() bool {
	return true
}

func (*DumbBalancer) HandoffList() []string {
	return make([]string, 0)
}

type CountBalancer struct {
	c *Cluster
	lk sync.Mutex
}

func (b *CountBalancer) upTo(workCount, nodesCount int) int {
	return int(math.Ceil(float64(workCount) / float64(nodesCount)))
}

func (b *CountBalancer) Init(c *Cluster) {
	b.c = c
}

func (b *CountBalancer) CanClaim() bool {
	b.lk.Lock()
	defer b.lk.Unlock()

	workCount := len(b.c.Work())
	claimedCount := len(b.c.Claimed())
	nodesCount := len(b.c.Nodes())
	if workCount <= 1 && claimedCount < workCount {
		return true
	}

	if claimedCount < b.upTo(workCount, nodesCount) {
		return true
	}

	return false
}

func (b *CountBalancer) HandoffList() []string {
	// for now just select from the front

	work := b.c.Work()
	workCount := len(b.c.Work())
	claimedCount := len(b.c.Claimed())
	nodesCount := len(b.c.Nodes())

	upTo := b.upTo(workCount, nodesCount)
	handoffCount := claimedCount - upTo
	if handoffCount > 0 {
		return work[:handoffCount]
	}

	return []string{}
}
