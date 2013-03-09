package donut

import (
	"launchpad.net/gozk/zookeeper"
)

type Listener interface {
	// Called when the listener joins a cluster
	OnJoin(*zookeeper.Conn)
	// Called when the listener leaved a cluster
	OnLeave()
	// Called when a task is started
	StartTask(string, map[string]interface{})
	// Called when a task is stopped
	EndTask(string)
}
