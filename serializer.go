package eventsource

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/altairsix/eventsource/internal"
	"github.com/golang/protobuf/proto"
)

// Serializer converts between Events and Records
type Serializer interface {
	// MarshalEvent converts an Event to a Record
	MarshalEvent(event Event) (Record, error)

	// UnmarshalEvent converts an Event backed into a Record
	UnmarshalEvent(record Record) (Event, error)
}

type jsonEvent struct {
	Type string          `json:"t"`
	Data json.RawMessage `json:"d"`
}

// JSONSerializer provides a simple serializer implementation
type JSONSerializer struct {
	eventTypes map[string]reflect.Type
}

// Bind registers the specified events with the serializer; may be called more than once
func (j *JSONSerializer) Bind(events ...Event) {
	for _, event := range events {
		eventType, t := EventType(event)
		j.eventTypes[eventType] = t
	}
}

// MarshalEvent converts an event into its persistent type, Record
func (j *JSONSerializer) MarshalEvent(v Event) (Record, error) {
	eventType, _ := EventType(v)

	data, err := json.Marshal(v)
	if err != nil {
		return Record{}, err
	}

	data, err = json.Marshal(jsonEvent{
		Type: eventType,
		Data: json.RawMessage(data),
	})
	if err != nil {
		return Record{}, NewError(err, ErrInvalidEncoding, "unable to encode event")
	}

	return Record{
		Version: v.EventVersion(),
		Data:    data,
	}, nil
}

// UnmarshalEvent converts the persistent type, Record, into an Event instance
func (j *JSONSerializer) UnmarshalEvent(record Record) (Event, error) {
	wrapper := jsonEvent{}
	err := json.Unmarshal(record.Data, &wrapper)
	if err != nil {
		return nil, NewError(err, ErrInvalidEncoding, "unable to unmarshal event")
	}

	t, ok := j.eventTypes[wrapper.Type]
	if !ok {
		return nil, NewError(err, ErrUnboundEventType, "unbound event type, %v", wrapper.Type)
	}

	v := reflect.New(t).Interface()
	err = json.Unmarshal(wrapper.Data, v)
	if err != nil {
		return nil, NewError(err, ErrInvalidEncoding, "unable to unmarshal event data into %#v", v)
	}

	return v.(Event), nil
}

// MarshalAll is a utility that marshals all the events provided into a History object
func (j *JSONSerializer) MarshalAll(events ...Event) (History, error) {
	history := make(History, 0, len(events))

	for _, event := range events {
		record, err := j.MarshalEvent(event)
		if err != nil {
			return nil, err
		}
		history = append(history, record)
	}

	return history, nil
}

// NewJSONSerializer constructs a new JSONSerializer and populates it with the specified events.
// Bind may be subsequently called to add more events.
func NewJSONSerializer(events ...Event) *JSONSerializer {
	serializer := &JSONSerializer{
		eventTypes: map[string]reflect.Type{},
	}
	serializer.Bind(events...)

	return serializer
}

func NewProtoSerializer() *ProtoSerializer {
	return &ProtoSerializer{}
}

type ProtoSerializer struct {
}

func (p *ProtoSerializer) MarshalEvent(v Event) (Record, error) {
	pb, ok := v.(proto.Message)
	if !ok {
		return Record{}, errors.New("Unable to marshal non proto event")
	}

	buf, err := proto.Marshal(pb)
	if err != nil {
		return Record{}, err
	}

	data, err := proto.Marshal(&internal.Envelope{
		Type: proto.MessageName(pb),
		Data: buf,
	})

	if err != nil {
		return Record{}, err
	}

	return Record{
		Version: v.EventVersion(),
		Data:    data,
	}, nil
}

func (p *ProtoSerializer) UnmarshalEvent(record Record) (Event, error) {
	var envelope internal.Envelope
	if err := proto.Unmarshal(record.Data, &envelope); err != nil {
		return nil, err
	}

	t := proto.MessageType(envelope.Type)
	if t == nil {
		return nil, fmt.Errorf("proto.MessageType unknown for %q", envelope.Type)
	}

	v := reflect.New(t.Elem()).Interface().(proto.Message)
	if err := proto.Unmarshal(envelope.Data, v); err != nil {
		return nil, err
	}

	event, ok := v.(Event)
	if !ok {
		return nil, fmt.Errorf("Unable to cast %T to Event", v)
	}
	return event, nil
}
