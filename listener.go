package donut

import (
	"gozk"
)

type Listener interface {
	// Called when the listener joins a cluster
	OnJoin(*gozk.ZooKeeper)
	// Called when the listener leaved a cluster
	OnLeave()
	// Called when work is started
	StartWork(string, map[string]interface{})
	// Called when work is stopped
	EndWork(string)
}

type MonitoredListener interface {
	Listener
	// Returns a map of information
	Information() map[string]interface{} // TODO: should be Marshaler?
	// Host to access API
	APIHost() string
	// Port to access API
	APIPort() string
}
