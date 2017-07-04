package eventsource

import "context"

// StreamRecord provides a serialized version of the event stream
type StreamRecord struct {
	Record
	Offset      uint64
	AggregateID string
}

// StreamReader allows one to query the raw event stream to read the next N events
// This is particular useful to publish events in cases where the underlying event store
// can't publish events on its own e.g. databases
type StreamReader interface {
	// Read reads the next recordCount records from the event store starting at the specified
	// offset
	Read(ctx context.Context, startingOffset uint64, recordCount int) ([]StreamRecord, error)
}

// StreamReaderFunc provides an func alternative for declaring a StreamReader
type StreamReaderFunc func(ctx context.Context, startingOffset uint64, recordCount int) ([]StreamRecord, error)

// Read implements the StreamReader.Read interface
func (fn StreamReaderFunc) Read(ctx context.Context, startingOffset uint64, recordCount int) ([]StreamRecord, error) {
	return fn(ctx, startingOffset, recordCount)
}
