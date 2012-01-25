package donut

import (
	"fmt"
	"encoding/json"
	"gozk"
	"log"
	"sync"
)

type SafeMap struct {
	_map map[string]interface{}
	lk   sync.RWMutex
}

func NewSafeMap(initial map[string]interface{}) *SafeMap {
	m := &SafeMap{
		_map: make(map[string]interface{}),
	}
	for key, val := range initial {
		m._map[key] = val
	}
	return m
}

func (m *SafeMap) Get(key string) interface{} {
	m.lk.RLock()
	defer m.lk.RUnlock()
	return m._map[key]
}

func (m *SafeMap) Contains(key string) bool {
	m.lk.RLock()
	defer m.lk.RUnlock()
	_, ok := m._map[key]
	return ok
}

func (m *SafeMap) Delete(key string) interface{} {
	m.lk.Lock()
	defer m.lk.Unlock()
	v := m._map[key]
	delete(m._map, key)
	return v
}

func (m *SafeMap) Put(key string, value interface{}) interface{} {
	m.lk.Lock()
	defer m.lk.Unlock()
	old := m._map[key]
	m._map[key] = value
	return old
}

func (m *SafeMap) RangeLock() map[string]interface{} {
	m.lk.RLock()
	return m._map
}

func (m *SafeMap) RangeUnlock() {
	m.lk.RUnlock()
}

func (m *SafeMap) Clear() {
	m.lk.Lock()
	defer m.lk.Unlock()
	m._map = make(map[string]interface{})
}

func (m *SafeMap) Len() int {
	m.lk.RLock()
	defer m.lk.RUnlock()
	return len(m._map)
}

func (m *SafeMap) Dump() string {
	m.lk.RLock()
	defer m.lk.RUnlock()
	rval := ""
	for k, v := range m._map {
		rval += fmt.Sprintf("(key: %s, value: %v)\n", k, v)
	}
	return rval
}

// Watch the children at path until a byte is sent on the returned channel
// Uses the SafeMap more like a set, so you'll have to use Contains() for entries
func watchZKChildren(zk *gozk.ZooKeeper, path string, children *SafeMap, onChange func(*SafeMap)) (chan byte, error) {
	initial, _, watch, err := zk.ChildrenW(path)
	if err != nil {
		return nil, err
	}
	m := children.RangeLock()
	for _, node := range initial {
		m[node] = nil
	}
	children.RangeUnlock()
	kill := make(chan byte)
	go func() {
		defer close(kill)
		var nodes []string
		var err error
		for {
			select {
			case <-kill:
				// close(watch)
				return
			case event := <-watch:
				if !event.Ok() {
					continue
				}
				nodes, _, watch, err = zk.ChildrenW(path)
				if err != nil {
					log.Printf("Error in watchZkChildren: %v", err)
					// XXX I should really provide some way for the client to find out about this error...
					return
				}
				m := children.RangeLock()
				// mark all dead
				for k := range m {
					m[k] = 0
				}
				for _, node := range nodes {
					m[node] = 1
				}
				for k, v := range m {
					if v.(int) == 0 {
						delete(m, k)
					}
				}
				children.RangeUnlock()
				onChange(children)
			}
		}
	}()
	log.Printf("watcher setup on %s", path)
	return kill, nil
}

func serializeCreate(zk *gozk.ZooKeeper, path string, data map[string]interface{}) (err error) {
	var e []byte
	if e, err = json.Marshal(data); err != nil {
		return
	}
	_, err = zk.Create(path, string(e), 0, gozk.WorldACL(gozk.PERM_ALL))
	return
}

func getDeserialize(zk *gozk.ZooKeeper, path string) (data map[string]interface{}, err error) {
	var e string
	e, _, err = zk.Get(path)
	if err != nil {
		log.Printf("error on get in getDeserialize for %s: %v", path, err)
		return
	}
	err = json.Unmarshal([]byte(e), &data)
	return
}
