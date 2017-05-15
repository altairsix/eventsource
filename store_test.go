package eventsource_test

import (
	"testing"

	"sort"

	"github.com/altairsix/eventsource"
	"github.com/stretchr/testify/assert"
)

func TestHistory_Swap(t *testing.T) {
	history := eventsource.History{
		{Version: 3},
		{Version: 1},
		{Version: 2},
	}

	sort.Sort(history)
	assert.Equal(t, 1, history[0].Version)
	assert.Equal(t, 2, history[1].Version)
	assert.Equal(t, 3, history[2].Version)
}
