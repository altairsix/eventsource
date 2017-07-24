package singleton

import (
	"context"

	"github.com/altairsix/eventsource"
)

// Dispatcher represents a function to execute a command
//
// Deprecated: Use Repository instead
type Dispatcher interface {
	// Dispatch calls the command using the repository associated with the dispatcher
	Dispatch(ctx context.Context, command eventsource.Command) error
}

// DispatcherFunc provides a convenience func form of Dispatcher
//
// Deprecated: Use RepositoryFunc instead
type DispatcherFunc func(ctx context.Context, command eventsource.Command) error

// Dispatch satisfies the Dispatcher interface
func (fn DispatcherFunc) Dispatch(ctx context.Context, command eventsource.Command) error {
	return fn(ctx, command)
}

// Repository represents a function to execute a command that returns the version number
// of the event after the command was applied
type Repository interface {
	Apply(ctx context.Context, command eventsource.Command) (int, error)
}

// RepositoryFunc provides a func convenience wrapper for Repository
type RepositoryFunc func(ctx context.Context, command eventsource.Command) (int, error)

// Apply satisfies the Repository interface
func (fn RepositoryFunc) Apply(ctx context.Context, command eventsource.Command) (int, error) {
	return fn(ctx, command)
}
