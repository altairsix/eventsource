package singleton

import (
	"context"

	"github.com/altairsix/eventsource"
)

// Dispatcher represents a function to execute a command
type Dispatcher interface {
	// Dispatch calls the command using the repository associated with the dispatcher
	Dispatch(ctx context.Context, command eventsource.Command) error
}

// DispatcherFunc provides a convenience func form of Dispatcher
type DispatcherFunc func(ctx context.Context, command eventsource.Command) error

// Dispatch satisfies the Dispatcher interface
func (fn DispatcherFunc) Dispatch(ctx context.Context, command eventsource.Command) error {
	return fn(ctx, command)
}
