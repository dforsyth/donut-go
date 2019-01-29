package cluster

type Balancer interface {
	// Can claim should return true if a node can attempt to claim a given
	// workKey, and false otherwise.
	CanClaim(string) bool
	// OnBalance is during a nodes balance step. Useful for bookeeping or
	// actions necessary to balance the cluster
	OnBalance()
}

type DumbBalancer struct{}

func (*DumbBalancer) CanClaim(workKey string) bool {
	return true
}

func (*DumbBalancer) OnBalance() {

}
