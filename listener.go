package donut

import (
	zkutil "github.com/dforsyth/sprinkles/zookeeper"
)

type Listener interface {
	// Called when the listener joins a cluster
	OnJoin(*zkutil.ZooKeeper)
	// Called when the listener leaved a cluster
	OnLeave()
	// Called when a task is started
	StartTask(string)
	// Called when a task is stopped
	EndTask(string)
}
