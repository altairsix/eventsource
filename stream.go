package eventsource

import "context"

// StreamRecord provides a serialized version of the event stream
type StreamRecord struct {
	Record
	Offset      int64
	AggregateID string
}

// StreamReader allows one to query the raw event stream to read the next N events
// This is particular useful to publish events in cases where the underlying event store
// can't publish events on its own e.g. databases
type StreamReader interface {
	// Read reads the next recordCount records from the event store starting at the specified
	// offset
	Read(ctx context.Context, startingOffset int64, recordCount int) ([]StreamRecord, error)
}
