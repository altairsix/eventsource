package eventsource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vancelongwill/eventsource"
)

type Custom struct {
	eventsource.Model
}

func (c Custom) EventType() string {
	return "blah"
}

func TestEventType(t *testing.T) {
	m := Custom{}
	eventType, _ := eventsource.EventType(m)
	assert.Equal(t, "blah", eventType)
}
