package cluster

import (
	"fmt"
)

type StateTransitionError struct {
	From, To string
}

func (e StateTransitionError) Error() string {
	return fmt.Sprintf("Cannot move from state[%s] to state[%s]", e.From, e.To)
}

type NotOwnedError struct {
	WorkKey string
}

func (e NotOwnedError) Error() string {
	return fmt.Sprintf("%s is not owned by this node", e.WorkKey)
}
