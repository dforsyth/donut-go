package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStoreEphemeral(t *testing.T) {
	cli := NewTestClient()
	coordinator := NewTestCoordinator(cli)

	ctx := context.TODO()

	assert.NoError(t, coordinator.StoreEphemeral("ephemeral", "value"))

	fr, err := cli.Get(ctx, "ephemeral")
	assert.NoError(t, err)
	assert.Equal(t, string(fr.Kvs[0].Value), "value")

	coordinator.Finish()
	cli.Close()

	sr, err := cli.Get(ctx, "ephemeral")
	assert.Error(t, err)
	assert.Nil(t, sr)
}

func TestStore(t *testing.T) {

}

func TestDelete(t *testing.T) {

}

func TestDeleteWithCondition(t *testing.T) {

}

func TestWatch(t *testing.T) {

}
