package eventsource

import (
	"encoding/json"
	"reflect"

	"github.com/vancelongwill/eventsource/pbevent"
	"google.golang.org/protobuf/proto"
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

type ProtoSerializer struct {
	eventTypes map[string]reflect.Type
}

// Bind registers the specified events with the serializer; may be called more than once
func (p *ProtoSerializer) Bind(events ...Event) {
	for _, event := range events {
		if _, ok := event.(proto.Message); ok {
			eventType, reflectType := EventType(event)
			p.eventTypes[eventType] = reflectType
		}
	}
}

// MarshalEvent converts an Event to a Record
func (p *ProtoSerializer) MarshalEvent(v Event) (Record, error) {
	pv, ok := v.(proto.Message)
	if !ok {
		return Record{}, NewError(nil, ErrInvalidEncoding, "event must implement the ProtoEventer interface")
	}
	data, err := proto.Marshal(pv)
	if err != nil {
		return Record{}, err
	}

	s, _ := EventType(v)

	data, err = proto.Marshal(&pbevent.Event{
		EventTypename: s,
		EventData:     data,
	})

	if err != nil {
		return Record{}, NewError(err, ErrInvalidEncoding, "unable to encode event")
	}

	return Record{
		Version: v.EventVersion(),
		Data:    data,
	}, nil
}

// UnmarshalEvent converts an Event backed into a Record
func (p *ProtoSerializer) UnmarshalEvent(record Record) (Event, error) {
	var evt pbevent.Event
	err := proto.Unmarshal(record.Data, &evt)
	if err != nil {
		return nil, NewError(err, ErrInvalidEncoding, "unable to unmarshal event")
	}

	t, ok := p.eventTypes[evt.EventTypename]
	if !ok {
		return nil, NewError(err, ErrUnboundEventType, "unbound event type, %v", evt.EventTypename)
	}

	n := reflect.New(t)
	c := n.Interface().(proto.Message)
	v := c.ProtoReflect().New().Interface()
	err = proto.Unmarshal(evt.EventData, v)
	if err != nil {
		return nil, NewError(err, ErrInvalidEncoding, "unable to unmarshal event data into %#v", v)
	}
	n.Elem().Field(0).Set(reflect.ValueOf(v))

	return n.Interface().(Event), nil
}

// NewJSONSerializer constructs a new JSONSerializer and populates it with the specified events.
// Bind may be subsequently called to add more events.
// Events must also fulfill the proto.Message interface.
func NewProtoSerializer(events ...Event) *ProtoSerializer {
	serializer := &ProtoSerializer{
		eventTypes: map[string]reflect.Type{},
	}
	serializer.Bind(events...)
	return serializer
}
