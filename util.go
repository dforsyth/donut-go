package donut

import (
	"encoding/json"
	"fmt"
	"launchpad.net/gozk/zookeeper"
	"log"
	"path"
	"sync"
)

// A locked map
type SafeMap struct {
	_map map[string]interface{}
	lk   sync.RWMutex
}

// Create a new SafeMap
func NewSafeMap(initial map[string]interface{}) *SafeMap {
	m := &SafeMap{
		_map: make(map[string]interface{}),
	}
	for key, val := range initial {
		m._map[key] = val
	}
	return m
}

// Get a value from the map
func (m *SafeMap) Get(key string) interface{} {
	m.lk.RLock()
	defer m.lk.RUnlock()
	return m._map[key]
}

// Check whether a key exists in the map
func (m *SafeMap) Contains(key string) bool {
	m.lk.RLock()
	defer m.lk.RUnlock()
	_, ok := m._map[key]
	return ok
}

// Remove a key from the map
func (m *SafeMap) Delete(key string) interface{} {
	m.lk.Lock()
	defer m.lk.Unlock()
	v := m._map[key]
	delete(m._map, key)
	return v
}

// Put a key, value into the map
func (m *SafeMap) Put(key string, value interface{}) interface{} {
	m.lk.Lock()
	defer m.lk.Unlock()
	old := m._map[key]
	m._map[key] = value
	return old
}

// Take an extended lock over the map
func (m *SafeMap) RangeLock() map[string]interface{} {
	m.lk.RLock()
	return m._map
}

// Release extended lock
func (m *SafeMap) RangeUnlock() {
	m.lk.RUnlock()
}

// Copy the map into a normal map
func (m *SafeMap) GetCopy() map[string]interface{} {
	m.lk.RLock()
	defer m.lk.RUnlock()
	_m := make(map[string]interface{})
	for k, v := range m._map {
		_m[k] = v
	}
	return _m
}

// Clear th map
func (m *SafeMap) Clear() {
	m.lk.Lock()
	defer m.lk.Unlock()
	m._map = make(map[string]interface{})
}

// Get the size of the map
func (m *SafeMap) Len() int {
	m.lk.RLock()
	defer m.lk.RUnlock()
	return len(m._map)
}

// Dump the map as a string
func (m *SafeMap) Dump() string {
	m.lk.RLock()
	defer m.lk.RUnlock()
	rval := ""
	for k, v := range m._map {
		rval += fmt.Sprintf("(key: %s, value: %v)\n", k, v)
	}
	return rval
}

// List all keys in the map
func (m *SafeMap) Keys() (keys []string) {
	_m := m.RangeLock()
	defer m.RangeUnlock()
	for k := range _m {
		keys = append(keys, k)
	}
	return
}

type ZKMap struct {
	_m       *SafeMap
	path     string
	zk       *zookeeper.Conn
	onChange func(*ZKMap)
}

func (m *ZKMap) Get(key string) interface{} {
	return m._m.Get(key)
}

func NewZKMap(zk *zookeeper.Conn, path string, onChange func(*ZKMap)) (*ZKMap, error) {
	m := &ZKMap{
		_m:       NewSafeMap(nil),
		path:     path,
		zk:       zk,
		onChange: onChange,
	}
	if err := m.watchChildren(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *ZKMap) watchChildren() error {
	children, _, watch, err := m.zk.ChildrenW(m.path)
	if err != nil {
		return err
	}

	go func() {
		event := <-watch
		switch event.Type {
		case zookeeper.EVENT_CHILD:
			if err := m.watchChildren(); err != nil {
				// XXX log this and try again
				panic("failed to watch children")
			}
		default:
			panic(event.String())
		}
	}()

	m.updateEntries(NewStringSet(children))
	return nil
}

func (m *ZKMap) updateEntries(children *StringSet) {
	keys := NewStringSet(m._m.Keys())
	added := children.Difference(keys)
	removed := keys.Difference(children)
	for _, k := range added {
		// TODO pull this all out into its own method
		p := path.Join(m.path, k)
		d, _, err := m.zk.Get(path.Join(p))
		if err != nil {
			panic("couldnt get " + p)
		}

		// unmashal and stuff it into the safemap
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(d), &data); err != nil {
		}
		m._m.Put(k, data)
	}
	for _, k := range removed {
		m._m.Delete(k)
	}
	if len(added) > 0 || len(removed) > 0 {
		m.onChange(m)
	}
}

type StringSet struct {
	m map[string]struct{}
}

func NewStringSet(initial []string) *StringSet {
	s := &StringSet{
		m: make(map[string]struct{}),
	}
	for _, e := range initial {
		s.m[e] = struct{}{}
	}
	return s
}

func (s *StringSet) Difference(o *StringSet) (difference []string) {
	for k := range s.m {
		if _, ok := o.m[k]; !ok {
			difference = append(difference, k)
		}
	}
	return
}

func serializeCreate(zk *zookeeper.Conn, path string, data map[string]interface{}) (err error) {
	var e []byte
	if e, err = json.Marshal(data); err != nil {
		return
	}
	_, err = zk.Create(path, string(e), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	return
}

func getDeserialize(zk *zookeeper.Conn, path string) (data map[string]interface{}, err error) {
	var e string
	e, _, err = zk.Get(path)
	if err != nil {
		log.Printf("error on get in getDeserialize for %s: %v", path, err)
		return
	}
	err = json.Unmarshal([]byte(e), &data)
	return
}

// Create a task in a cluster
func CreateTask(clusterName string, zk *zookeeper.Conn, config *Config, taskId string, data map[string]interface{}) (err error) {
	p := path.Join("/", clusterName, config.TasksPath, taskId)
	if err = serializeCreate(zk, p, data); err != nil {
		log.Printf("Failed to create task %s (%s): %v", taskId, p, err)
	} else {
		log.Printf("Created task %s", p)
	}
	return
}

// Remove a task from a cluster
func CompleteTask(clusterName string, zk *zookeeper.Conn, config *Config, taskId string) {
	p := path.Join("/", clusterName, config.TasksPath, taskId)
	zk.Delete(p, -1)
	log.Printf("Deleted task %s (%s)", taskId, p)
}
