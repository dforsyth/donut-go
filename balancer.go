package donut

type Balancer interface {
	Init(l Listener)
	AddWork(string)
	RemoveWork(string)
	CanClaim() bool
	HandoffList() []string
}
