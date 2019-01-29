package client

import (
	"github.com/dforsyth/donut/cluster"
	"github.com/dforsyth/donut/coordinator"
)

type Client struct {
	kv coordinator.Coordinator
}

func New(kv coordinator.Coordinator) *Client {
	return &Client{kv: kv}
}

func (c *Client) CreateWork(cls *cluster.Cluster, name, value string) (string, error) {
	workKey := cluster.Key(cls, cluster.TypeWork, name)
	if err := c.kv.Store(workKey, value); err != nil {
		return "", err
	}
	return workKey, nil
}

func (c *Client) DeleteWork(cls *cluster.Cluster, name string) error {
	workKey := cluster.Key(cls, cluster.TypeWork, name)
	return c.kv.Delete(workKey)
}
