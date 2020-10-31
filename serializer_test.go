package eventsource_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vancelongwill/eventsource"
	"github.com/vancelongwill/eventsource/pbevent"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type EntitySetName struct {
	eventsource.Model
	Name string
}

func TestJSONSerializer(t *testing.T) {
	event := EntitySetName{
		Model: eventsource.Model{
			ID:      "123",
			Version: 456,
		},
		Name: "blah",
	}

	serializer := eventsource.NewJSONSerializer(event)
	record, err := serializer.MarshalEvent(event)
	assert.Nil(t, err)
	assert.NotNil(t, record)

	v, err := serializer.UnmarshalEvent(record)
	assert.Nil(t, err)

	found, ok := v.(*EntitySetName)
	assert.True(t, ok)
	assert.Equal(t, &event, found)
}

func TestJSONSerializer_MarshalAll(t *testing.T) {
	event := EntitySetName{
		Model: eventsource.Model{
			ID:      "123",
			Version: 456,
		},
		Name: "blah",
	}

	serializer := eventsource.NewJSONSerializer(event)
	history, err := serializer.MarshalAll(event)
	assert.Nil(t, err)
	assert.NotNil(t, history)

	v, err := serializer.UnmarshalEvent(history[0])
	assert.Nil(t, err)

	found, ok := v.(*EntitySetName)
	assert.True(t, ok)
	assert.Equal(t, &event, found)
}

type UserCreatedEvent struct {
	// used here as we dont have any other proto messages for testing purposes.
	// checkout ./_examples/fullproto for a real working example
	*pbevent.EventMeta
}

func (u UserCreatedEvent) AggregateID() string {
	return u.Id
}

func (u UserCreatedEvent) EventVersion() int {
	return int(u.Version)
}

func (u UserCreatedEvent) EventAt() time.Time {
	return u.At.AsTime()
}

func TestProtoSerializer(t *testing.T) {
	createdAt := timestamppb.Now()
	event := UserCreatedEvent{
		&pbevent.EventMeta{
			Id:      "123",
			Version: 1,
			At:      createdAt,
		},
	}
	serializer := eventsource.NewProtoSerializer(event)
	record, err := serializer.MarshalEvent(event)
	assert.Nil(t, err)
	assert.NotNil(t, record)

	v, err := serializer.UnmarshalEvent(record)
	assert.Nil(t, err)

	found, ok := v.(*UserCreatedEvent)
	assert.True(t, ok)
	assert.True(t, proto.Equal(&event, found))
}
