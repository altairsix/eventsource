package eventsource_test

import (
	"testing"

	"github.com/altairsix/eventsource"
	"github.com/stretchr/testify/assert"
)

func TestCommandModel_AggregateID(t *testing.T) {
	m := eventsource.CommandModel{ID: "abc"}
	assert.Equal(t, m.ID, m.AggregateID())
}
