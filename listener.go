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
