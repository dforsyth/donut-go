package coordinator

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"sync"
	"time"

	// Wait for etcd client v3.4, there is a module import issue.
	client "github.com/coreos/etcd/clientv3"                  // "go.etcd.io/etcd/clientv3"
	clientutil "github.com/coreos/etcd/clientv3/clientv3util" // "go.etcd.io/etcd/clientv3/clientv3util"
	pb "github.com/coreos/etcd/mvcc/mvccpb"                   // "go.etcd.io/etcd/mvcc/mvccpb"
	"github.com/dforsyth/donut/log"
)

// Coordinator is a storage interface
type Coordinator interface {
	Store(key, value string) error
	// Store a value at key, with a lifetime attached to client liveness
	StoreEphemeral(key, value string) error
	// Load(key string) (string, error)
	// Delete a key
	Delete(key string, ifs ...client.Cmp) error
	// Watch a key, populating a map with all values within that keys child
	// space
	Watch(prefix string, handler WatchMapChangeHandler) (*WatchMap, error)
	// Finish stops a Coordinator
	Finish() error
}

// EtcdCoordinator is a coordinator based on Etcd
type EtcdCoordinator struct {
	client       *client.Client
	leaseID      client.LeaseID
	timeout      time.Duration
	leaseTimeout time.Duration
	logger       log.FmtLogger
}

type EtcdCoordinatorOption func(*EtcdCoordinator)

func WithLogger(logger log.FmtLogger) func(*EtcdCoordinator) {
	return func(kv *EtcdCoordinator) { kv.logger = logger }
}

func WithRequestTimeout(requestTimeout time.Duration) func(*EtcdCoordinator) {
	return func(kv *EtcdCoordinator) { kv.timeout = requestTimeout }
}

func WithLeaseTimeout(leaseTimeout time.Duration) func(*EtcdCoordinator) {
	return func(kv *EtcdCoordinator) { kv.leaseTimeout = leaseTimeout }
}

func NewEtcdCoordinator(client *client.Client, opts ...EtcdCoordinatorOption) (*EtcdCoordinator, error) {
	kv := &EtcdCoordinator{
		client:       client,
		timeout:      1 * time.Second,
		leaseTimeout: 5 * time.Second,
		logger:       stdlog.New(os.Stderr, "", stdlog.LstdFlags),
	}
	for _, opt := range opts {
		opt(kv)
	}
	_ctx, cancel := context.WithTimeout(context.Background(), kv.timeout)
	leaseResp, err := client.Grant(_ctx, int64(kv.leaseTimeout.Seconds()))
	cancel()
	if err != nil {
		return nil, err
	}

	ch, err := client.KeepAlive(context.Background(), leaseResp.ID)
	if err != nil {
		return nil, fmt.Errorf("Keep alive error: %s", err)
	}
	go func() {
		for r := range ch {
			kv.logger.Printf("Keep alive response: %d", r.ID)
		}
	}()

	kv.leaseID = leaseResp.ID

	return kv, nil
}

func (kv *EtcdCoordinator) Finish() error {
	// We don't close the client in here since we didn't create it here.
	return nil
}

func (kv *EtcdCoordinator) Store(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), kv.timeout)
	_, err := kv.client.Txn(ctx).
		If(clientutil.KeyMissing(key)).
		Then(client.OpPut(key, value)).
		Commit()
	cancel()
	if err != nil {
		return err
	}
	return nil
}

func (kv *EtcdCoordinator) StoreEphemeral(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), kv.timeout)
	resp, err := kv.client.Txn(ctx).
		If(clientutil.KeyMissing(key)).
		Then(client.OpPut(key, value, client.WithLease(kv.leaseID))).
		Commit()
	cancel()
	if !resp.Succeeded {
		return fmt.Errorf("Failed to store %s in txn", key)
	}
	fmt.Printf("Stored %s\n", key)
	if err != nil {
		return err
	}
	return nil
}

func (kv *EtcdCoordinator) Delete(key string, cmps ...client.Cmp) error {
	ctx, cancel := context.WithTimeout(context.Background(), kv.timeout)
	resp, err := kv.client.Txn(ctx).
		If(cmps...).
		Then(client.OpDelete(key)).
		Commit()
	cancel()
	if !resp.Succeeded {
		return fmt.Errorf("Failed to delete %s in txn", key)
	}
	if err != nil {
		return err
	}
	return nil
}

// Watch a given prefix, firing handler on a change. Returns a WatchMap.
func (kv *EtcdCoordinator) Watch(prefix string, handler WatchMapChangeHandler) (*WatchMap, error) {
	_ctx, _cancel := context.WithCancel(context.Background())
	resp, err := kv.client.Get(_ctx, prefix, client.WithPrefix())
	_cancel()
	if err != nil {
		kv.logger.Printf("Failed to get initial for watch %s", err)
		return nil, err
	}

	data := make(map[string]string)
	for _, kv := range resp.Kvs {
		data[string(kv.Key)] = string(kv.Value)
	}

	watcher := client.NewWatcher(kv.client)
	watchChan := watcher.Watch(
		context.Background(),
		prefix,
		client.WithPrefix(),
		client.WithRev(resp.Header.GetRevision()),
	)
	ctx, cancel := context.WithCancel(context.Background())

	nm := &WatchMap{
		m:      &sync.RWMutex{},
		data:   data,
		cancel: cancel,
	}

	// Run handler on the initial map
	go handler(nm)

	// Start the watch loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				kv.logger.Printf("Watch done")
				watcher.Close()
				return
			case wr := <-watchChan:
				// If we're canceled, bail
				if wr.Canceled {
					return
				}
				for _, ev := range wr.Events {
					k := string(ev.Kv.Key)
					v := string(ev.Kv.Value)
					kv.logger.Printf("(%s) Watch event %s %s:%s", prefix, ev.Type, k, v)
					switch ev.Type {
					case pb.PUT:
						nm.set(k, v)
					case pb.DELETE:
						nm.delete(k)
					}
				}

			}
			go handler(nm)
		}
	}()

	return nm, nil
}
