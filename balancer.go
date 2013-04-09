package donut

type Balancer interface {
	// Prepare the balancer to start balancing
	Init(*Cluster)
	// Indicates whether the listener that this balancer is attached to can claim a given task or not
	CanClaim(string) bool
	// Work to be released by this listener in a rebalance
	HandoffList() []string
}

type DumbBalancer struct {
}

func (*DumbBalancer) Init(c *Cluster) {
}

func (*DumbBalancer) CanClaim(taskId string) bool {
	return true
}

func (*DumbBalancer) HandoffList() []string {
	return make([]string, 0)
}
