package eventsource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

// Aggregate represents the aggregate root in the domain driven design sense.
// It represents the current state of the domain object and can be thought of
// as a left fold over events.
type Aggregate interface {
	// On will be called for each event; returns err if the event could not be
	// applied
	On(event Event) error
}

// Repository provides the primary abstraction to saving and loading events
type Repository struct {
	prototype  reflect.Type
	store      Store
	serializer Serializer
	observers  []func(Event)
	writer     io.Writer
	debug      bool
}

// Option provides functional configuration for a *Repository
type Option func(*Repository)

// WithDebug will generate additional logging useful for debugging
func WithDebug(w io.Writer) Option {
	return func(r *Repository) {
		r.writer = w
		r.debug = true
	}
}

// WithStore allows the underlying store to be specified; by default the repository
// uses an in-memory store suitable for testing only
func WithStore(store Store) Option {
	return func(r *Repository) {
		r.store = store
	}
}

// WithSerializer specifies the serializer to be used
func WithSerializer(serializer Serializer) Option {
	return func(r *Repository) {
		r.serializer = serializer
	}
}

// WithObservers allows observers to watch the saved events; Observers should invoke very short lived operations as
// calls will block until the observer is finished
func WithObservers(observers ...func(event Event)) Option {
	return func(r *Repository) {
		r.observers = append(r.observers, observers...)
	}
}

// New creates a new Repository using the JSONSerializer and MemoryStore
func New(prototype Aggregate, opts ...Option) *Repository {
	t := reflect.TypeOf(prototype)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	r := &Repository{
		prototype:  t,
		store:      newMemoryStore(),
		serializer: NewJSONSerializer(),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *Repository) logf(format string, args ...interface{}) {
	if !r.debug {
		return
	}

	now := time.Now().Format(time.StampMilli)
	io.WriteString(r.writer, now)
	io.WriteString(r.writer, " ")

	fmt.Fprintf(r.writer, format, args...)
	if !strings.HasSuffix(format, "\n") {
		io.WriteString(r.writer, "\n")
	}
}

// New returns a new instance of the aggregate
func (r *Repository) New() Aggregate {
	return reflect.New(r.prototype).Interface().(Aggregate)
}

// Save persists the events into the underlying Store
func (r *Repository) Save(ctx context.Context, events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	aggregateID := events[0].AggregateID()

	history := make(History, 0, len(events))
	for _, event := range events {
		record, err := r.serializer.MarshalEvent(event)
		if err != nil {
			return err
		}

		history = append(history, record)
	}

	return r.store.Save(ctx, aggregateID, history...)
}

// Load retrieves the specified aggregate from the underlying store
func (r *Repository) Load(ctx context.Context, aggregateID string) (Aggregate, error) {
	history, err := r.store.Load(ctx, aggregateID, 0, 0)
	if err != nil {
		return nil, err
	}

	entryCount := len(history)
	if entryCount == 0 {
		return nil, NewError(nil, ErrAggregateNotFound, "unable to load %v, %v", r.New(), aggregateID)
	}

	r.logf("Loaded %v event(s) for aggregate id, %v", entryCount, aggregateID)
	aggregate := r.New()

	for _, record := range history {
		event, err := r.serializer.UnmarshalEvent(record)
		if err != nil {
			return nil, err
		}

		err = aggregate.On(event)
		if err != nil {
			eventType, _ := EventType(event)
			return nil, NewError(err, ErrUnhandledEvent, "aggregate was unable to handle event, %v", eventType)
		}
	}

	return aggregate, nil
}

func (r *Repository) Dispatch(ctx context.Context, command Command) error {
	if command == nil {
		return errors.New("Command provided to Repository.Dispatch may not be nil")
	}
	aggregateID := command.AggregateID()
	if aggregateID == "" {
		return errors.New("Command provided to Repository.Dispatch may not contain a blank AggregateID")
	}

	aggregate, err := r.Load(ctx, aggregateID)
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

	err = r.Save(ctx, events...)
	if err != nil {
		return err
	}

	// publish events to observers
	if r.observers != nil {
		for _, event := range events {
			for _, observer := range r.observers {
				observer(event)
			}
		}
	}

	return nil
}

// Store returns the underlying Store
func (r *Repository) Store() Store {
	return r.store
}

// Serializer returns the underlying serializer
func (r *Repository) Serializer() Serializer {
	return r.serializer
}
