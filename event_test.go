package eventsource_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vancelongwill/eventsource"
)

func TestEvent(t *testing.T) {
	m := eventsource.Model{
		ID:      "abc",
		Version: 123,
		At:      time.Now(),
	}

	assert.Equal(t, m.ID, m.AggregateID())
	assert.Equal(t, m.Version, m.EventVersion())
	assert.Equal(t, m.At, m.EventAt())
}
