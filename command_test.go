package eventsource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vancelongwill/eventsource"
)

func TestCommandModel_AggregateID(t *testing.T) {
	m := eventsource.CommandModel{ID: "abc"}
	assert.Equal(t, m.ID, m.AggregateID())
}
