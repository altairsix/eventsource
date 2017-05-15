package eventsource

import "time"

// Event describe a change that happened to the Aggregate
//
// * Past tense e.g. EmailChanged
// * Contains intent e.g. EmailChanged is better than EmailSet
type Event interface {
	// AggregateID returns the id of the aggregate referenced by the event
	AggregateID() string

	// EventVersion contains the version number of this event
	EventVersion() int

	// EventAt indicates when the event occurred
	EventAt() time.Time
}

// EventTyper is an optional interface that an Event can implement that allows it to specify an event type
// different than the name of the struct
type EventTyper interface {
	// EventType returns the name of event type
	EventType() string
}

// Model provides a default implementation of an Event
type Model struct {
	// ID contains the AggregateID
	ID string

	// Version contains the EventVersion
	Version int

	// At contains the EventAt
	At time.Time
}

// AggregateID implements the Event interface
func (m Model) AggregateID() string {
	return m.ID
}

// EventVersion implements the Event interface
func (m Model) EventVersion() int {
	return m.Version
}

// EventAt implements the Event interface
func (m Model) EventAt() time.Time {
	return m.At
}
