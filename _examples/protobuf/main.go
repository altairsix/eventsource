package main

import (
	"fmt"
	"log"
)

//go:generate protoc --go_out=. events.proto
//go:generate protoc --plugin=protoc-gen-custom=$GOPATH/bin/eventsource-protobuf --custom_out=. events.proto

func check(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	serializer := NewSerializer()
	event := &ItemAdded{Id: "abc"}

	record, err := serializer.MarshalEvent(event)
	check(err)

	actual, err := serializer.UnmarshalEvent(record)
	check(err)

	if event.Id != actual.AggregateID() {
		check(fmt.Errorf("expected %#v; got %#v", event, actual))
	}
}
