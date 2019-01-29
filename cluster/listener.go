package cluster

import (
	"context"
)

// Listener is the interface for user actions on a Cluster.
type Listener interface {
	// Called when the listener joins a cluster
	OnJoin(c *Cluster)
	// Called when the listener leaves a cluster
	OnLeave()
	// Called when work is started. Observe the passed context to finish work.
	StartWork(context.Context, string)
}
