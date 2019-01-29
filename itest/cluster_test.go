package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3" // "go.etcd.io/etcd/clientv3"
	"github.com/dforsyth/donut/client"
	"github.com/dforsyth/donut/cluster"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/sync/syncmap"
)

func TestNew(t *testing.T) {
	assert.Nil(t, nil, "Sanity")
}

// TODO(dforsyth): Generate this
type mockListener struct {
	mock.Mock
}

func newMockListener() *mockListener {
	lst := &mockListener{}
	lst.On("OnJoin").Return()
	lst.On("OnLeave").Return()
	lst.On("StartWork", mock.Anything, mock.Anything).Return()
	return lst
}

func (l *mockListener) OnJoin(_ *cluster.Cluster) {
	l.Called()
}
func (l *mockListener) OnLeave() {
	l.Called()
}
func (l *mockListener) StartWork(ctx context.Context, wk string) {
	l.Called(ctx, wk)
}

func TestJoinAndLeave(t *testing.T) {
	cli := NewTestClient()
	kv := NewTestCoordinator(cli)
	id := uuid.New().String()
	nodeID := fmt.Sprintf("test-node-%s-0", id)
	lst := newMockListener()
	cls := cluster.New(
		fmt.Sprintf("test-%s", id),
		nodeID, lst,
	)
	err := cls.Join(kv)
	assert.Nil(t, err, "Should join with no error")

	nodeKey := cluster.Key(cls, cluster.TypeNode, nodeID)

	ctx := context.TODO()
	_, gerr := cli.Get(ctx, nodeKey)
	assert.NoError(t, gerr)

	lst.AssertCalled(t, "OnJoin")

	cls.Leave()

	resp, lerr := cli.Get(ctx, nodeKey)
	assert.NoError(t, lerr)
	assert.Equal(t, 0, len(resp.Kvs))

	lst.AssertCalled(t, "OnLeave")
}

func TestClaimWork(t *testing.T) {
	cli := NewTestClient()
	kv := NewTestCoordinator(cli)
	id := uuid.New().String()
	lst := newMockListener()
	cls := cluster.New(
		fmt.Sprintf("test-%s", id),
		fmt.Sprintf("test-node-%s-0", id),
		lst,
	)
	err := cls.Join(kv)
	defer cls.Leave()
	assert.Nil(t, err, "Should join with no error")

	ccli := client.New(kv)
	wk, cerr := ccli.CreateWork(cls, "test-work", "")
	assert.NoError(t, cerr)

	// Wait for the claim procedures. If this flakes, increase the time on this.
	time.Sleep(1 * time.Second)

	_, ok := cls.GetOwned().Load(wk)
	assert.True(t, ok)

	lst.AssertCalled(t, "StartWork", mock.Anything, wk)
}

type neverClaimBalancer struct{}

func (b *neverClaimBalancer) CanClaim(_ string) bool { return false }
func (b *neverClaimBalancer) OnBalance()             {}

type neverReclaimBalancerListener struct {
	claimed syncmap.Map
}

func (b *neverReclaimBalancerListener) CanClaim(wk string) bool {
	_, ok := b.claimed.Load(wk)
	return !ok
}
func (b *neverReclaimBalancerListener) OnBalance()                  {}
func (b *neverReclaimBalancerListener) OnJoin(cls *cluster.Cluster) {}
func (b *neverReclaimBalancerListener) OnLeave()                    {}
func (b *neverReclaimBalancerListener) StartWork(ctx context.Context, workKey string) {
	b.claimed.Store(workKey, nil)
	select {
	case <-ctx.Done():
	}
}

func TestEndwork(t *testing.T) {
	var bl neverReclaimBalancerListener
	cls := cluster.New(
		"testendwork",
		"testendwork",
		&bl,
		cluster.WithBalancer(&bl),
	)
	cli := NewTestClient()
	coo := NewTestCoordinator(cli)
	ccli := client.New(coo)
	cls.Join(coo)

	wk, cerr := ccli.CreateWork(cls, "testendwork", "")
	assert.NoError(t, cerr)

	// Sleep for the claim
	time.Sleep(1 * time.Second)
	_, cok := cls.GetOwned().Load(wk)
	assert.True(t, cok)

	assert.NoError(t, cls.EndWork(wk))
	time.Sleep(1 * time.Second)

	_, eok := cls.GetOwned().Load(wk)
	assert.False(t, eok)

	resp, eerr := cli.Get(context.TODO(), cluster.Key(cls, cluster.TypeClaim, wk))
	assert.NoError(t, eerr)
	assert.Equal(t, 0, len(resp.Kvs))
}

func TestEndworkNotOwned(t *testing.T) {
	var l neverClaimBalancer
	cls := cluster.New(
		"testendworknotowned",
		"testendworknotowned",
		newMockListener(),
		cluster.WithBalancer(&l),
	)
	coo := NewTestCoordinator(NewTestClient())
	assert.NoError(t, cls.Join(NewTestCoordinator(NewTestClient())))

	err := cls.EndWork("imaginary-work")
	assert.Error(t, err)

	cls.Leave()
	coo.Finish()
}

func TestEndworkOwnedButNoClaim(t *testing.T) {
	cls := cluster.New(
		"testendworknoclaim",
		"testendworknoclaim",
		newMockListener(),
	)
	cli := NewTestClient()
	coo := NewTestCoordinator(cli)
	ccli := client.New(coo)
	assert.NoError(t, cls.Join(coo))

	wk, cerr := ccli.CreateWork(cls, "testendworknoclaim", "")
	assert.NoError(t, cerr)

	// Sleep for the claim
	time.Sleep(1 * time.Second)
	_, cok := cls.GetOwned().Load(wk)
	assert.True(t, cok)

	// Drop the claim key
	ck := cluster.Key(cls, cluster.TypeClaim, wk)
	leases, lerr := cli.Leases(context.TODO())
	assert.NoError(t, lerr)
	_, perr := cli.Put(context.TODO(), ck, "imaginary", clientv3.WithLease(leases.Leases[0].ID))
	assert.NoError(t, perr)

	err := cls.EndWork(wk)
	assert.Error(t, err)
}
