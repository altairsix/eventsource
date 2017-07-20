package scenario

import (
	"context"
	"reflect"
	"testing"

	"github.com/altairsix/eventsource"
	"github.com/stretchr/testify/assert"
)

type AggregateCommandHandler interface {
	eventsource.Aggregate
	eventsource.CommandHandler
}

type Builder struct {
	t         *testing.T
	aggregate AggregateCommandHandler
	given     []eventsource.Event
	command   eventsource.Command
}

func (b *Builder) clone() *Builder {
	return &Builder{
		t:         b.t,
		aggregate: b.aggregate,
		given:     b.given,
		command:   b.command,
	}
}

func (b *Builder) Given(given ...eventsource.Event) *Builder {
	dupe := b.clone()
	dupe.given = append(dupe.given, given...)
	return dupe
}

func (b *Builder) When(command eventsource.Command) *Builder {
	dupe := b.clone()
	dupe.command = command
	return dupe
}

func (b *Builder) apply() ([]eventsource.Event, error) {
	t := reflect.TypeOf(b.aggregate)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	aggregate := reflect.New(t).Interface().(AggregateCommandHandler)

	// given
	for _, e := range b.given {
		assert.Nil(b.t, aggregate.On(e))
	}

	// when
	ctx := context.Background()
	return aggregate.Apply(ctx, b.command)
}

func (b *Builder) Then(expected ...eventsource.Event) {
	actual, err := b.apply()
	assert.Nil(b.t, err)

	// then
	if len(expected) != len(actual) {
		assert.Equal(b.t, expected, actual)
		return
	}

	for index, e := range expected {
		a := actual[index]
		assert.Equal(b.t, reflect.TypeOf(e), reflect.TypeOf(a))
	}
}

func (b *Builder) ThenError(fn func(err error) bool) {
	_, err := b.apply()
	assert.True(b.t, fn(err))
}

func New(t *testing.T, prototype AggregateCommandHandler) *Builder {
	return &Builder{
		t:         t,
		aggregate: prototype,
	}
}
