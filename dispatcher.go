package eventsource

import (
	"context"
	"fmt"
)

// Dispatcher represents a function to execute a command
type Dispatcher interface {
	// Dispatch calls the command using the repository associated with the dispatcher
	Dispatch(ctx context.Context, command Command) error
}

// DispatcherFunc provides a convenience func form of Dispatcher
type DispatcherFunc func(ctx context.Context, command Command) error

// Dispatch satisfies the Dispatcher interface
func (fn DispatcherFunc) Dispatch(ctx context.Context, command Command) error {
	return fn(ctx, command)
}

// NewDispatcher creates a new Dispatcher associated with the specified Repository
func NewDispatcher(r *Repository) Dispatcher {
	return DispatcherFunc(func(ctx context.Context, command Command) error {
		aggregate, err := r.Load(ctx, command.AggregateID())
		if err != nil {
			aggregate = r.New()
		}

		h, ok := aggregate.(CommandHandler)
		if !ok {
			return fmt.Errorf("Aggregate, %v, does not implement CommandHandler", aggregate)
		}
		events, err := h.Apply(ctx, command)
		if err != nil {
			return err
		}

		return r.Save(ctx, events...)
	})
}
