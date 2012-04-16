package donut

type Balancer interface {
	Init(c *Cluster)
	CanClaim() bool
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
