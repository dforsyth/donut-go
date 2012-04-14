package donut

type Balancer interface {
	Init(l Listener)
	AddWork(string)
	RemoveWork(string)
	CanClaim() bool
	HandoffList() []string
}

type DumbBalancer struct {
}

func (*DumbBalancer) Init(l Listener) {

}

func (*DumbBalancer) AddWork(id string) {

}

func (*DumbBalancer) RemoveWork(id string) {

}

func (*DumbBalancer) CanClaim() bool {
	return true
}

func (*DumbBalancer) HandoffList() []string {
	return make([]string, 0)
}
