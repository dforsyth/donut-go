package coordinator

import (
	"context"
	"sync"
	// "go.etcd.io/etcd/client"
)

// WatchMap provides an mapped interface to a watched range.
type WatchMap struct {
	m      *sync.RWMutex
	data   map[string]string
	cancel context.CancelFunc
}

// WatchMapChangeHandler callback.
type WatchMapChangeHandler func(*WatchMap)

// Get returns a value from a WatchMap. If a value is not present,
// bool will be false.
func (m *WatchMap) Get(key string) (string, bool) {
	m.m.RLock()
	defer m.m.RUnlock()

	v, ok := m.data[key]
	return v, ok
}

// Keys returns all keys from a NodeMap.
func (m *WatchMap) Keys() []string {
	m.m.RLock()
	defer m.m.RUnlock()

	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func (m *WatchMap) set(key, value string) {
	m.m.Lock()
	defer m.m.Unlock()

	m.data[key] = value
}

func (m *WatchMap) delete(key string) {
	m.m.Lock()
	defer m.m.Unlock()

	delete(m.data, key)
}

// Cancel will cancel a maps watch.
func (m *WatchMap) Cancel() {
	// TODO(dforsyth): Rework this so this function waits until the watcher
	// loop exits?
	m.cancel()
}
