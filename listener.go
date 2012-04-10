package donut

import (
	"gozk"
)

type Listener interface {
	OnJoin(*gozk.ZooKeeper)
	OnLeave()
	StartWork(string, map[string]interface{})
	EndWork(string)
}

type MonitoredListener interface {
	Listener
	Information() map[string]interface{} // TODO: should be Marshaler?
	APIHost() string
	APIPort() string
}
