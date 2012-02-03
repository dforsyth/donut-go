package donut

type Balancer interface {
	Init(*Cluster)
	AddWork(string)
	RemoveWork(string)
	CanClaim() bool
}
