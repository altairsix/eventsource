package scenario

import (
	"context"
	"reflect"

	"github.com/altairsix/eventsource"
	"github.com/stretchr/testify/assert"
)

// CommandHandlerAggregate implements both Aggregate and CommandHandler
type CommandHandlerAggregate interface {
	eventsource.CommandHandler
	eventsource.Aggregate
}

// Builder captures the data used to execute a test scenario
type Builder struct {
	t         assert.TestingT
	aggregate CommandHandlerAggregate
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

// Given allows an initial set of events to be provided; may be called multiple times
func (b *Builder) Given(given ...eventsource.Event) *Builder {
	dupe := b.clone()
	dupe.given = append(dupe.given, given...)
	return dupe
}

// When provides the command to test
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
	aggregate := reflect.New(t).Interface().(CommandHandlerAggregate)

	// given
	for _, e := range b.given {
		assert.Nil(b.t, aggregate.On(e))
	}

	// when
	ctx := context.Background()
	return aggregate.Apply(ctx, b.command)
}

func deepEquals(t assert.TestingT, expected, actual interface{}) bool {
	te := reflect.TypeOf(expected)
	ta := reflect.TypeOf(actual)
	if !assert.Equal(t, te, ta) {
		return false
	}

	if te.Kind() == reflect.Ptr {
		te = te.Elem()
	}

	ve := reflect.ValueOf(expected)
	if ve.Kind() == reflect.Ptr {
		ve = ve.Elem()
	}

	va := reflect.ValueOf(actual)
	if va.Kind() == reflect.Ptr {
		va = va.Elem()
	}

	for i := 0; i < te.NumField(); i++ {
		fieldType := te.Field(i).Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}

		fe := ve.Field(i)
		fa := va.Field(i)

		if !fe.CanInterface() || !fa.CanInterface() {
			continue
		}
		if zero := reflect.Zero(fieldType).Interface(); zero == fe.Interface() {
			continue
		}

		if fieldType.Kind() == reflect.Struct {
			if ok := deepEquals(t, fe.Interface(), fa.Interface()); !ok {
				return false
			}
			continue
		}

		if ok := assert.Equal(t, fe.Interface(), fa.Interface()); !ok {
			return false
		}
	}

	return true
}

// Then check that the command returns the following events.  Only non-zero valued
// fields will be checked.  If no non-zeroed values are present, then only the
// event type will be checked
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
		deepEquals(b.t, e, a)
	}
}

// ThenError verifies that the error returned by the command matches
// the function expectation
func (b *Builder) ThenError(matches func(err error) bool) {
	_, err := b.apply()
	assert.True(b.t, matches(err))
}

// New constructs a new scenario
func New(t assert.TestingT, prototype CommandHandlerAggregate) *Builder {
	return &Builder{
		t:         t,
		aggregate: prototype,
	}
}
