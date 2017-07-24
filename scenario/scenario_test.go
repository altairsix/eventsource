package scenario_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/altairsix/eventsource"
	"github.com/altairsix/eventsource/scenario"
	"github.com/stretchr/testify/assert"
)

//Order is an example of state generated from left fold of events
type Order struct {
	ID        string
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
	State     string
}

//OrderCreated event used a marker of order created
type OrderCreated struct {
	eventsource.Model
}

//OrderShipped event used a marker of order shipped
type OrderShipped struct {
	eventsource.Model
}

//On implements Aggregate interface
func (item *Order) On(event eventsource.Event) error {
	switch v := event.(type) {
	case *OrderCreated:
		item.State = "created"

	case *OrderShipped:
		item.State = "shipped"

	default:
		return fmt.Errorf("unable to handle event, %v", v)
	}

	item.Version = event.EventVersion()
	item.ID = event.AggregateID()
	item.UpdatedAt = event.EventAt()

	return nil
}

//CreateOrder command
type CreateOrder struct {
	eventsource.CommandModel
}

//ShipOrder command
type ShipOrder struct {
	eventsource.CommandModel
}

func (item *Order) Apply(ctx context.Context, command eventsource.Command) ([]eventsource.Event, error) {
	switch v := command.(type) {
	case *CreateOrder:
		orderCreated := &OrderCreated{
			Model: eventsource.Model{ID: command.AggregateID(), Version: item.Version + 1, At: time.Now()},
		}
		return []eventsource.Event{orderCreated}, nil

	case *ShipOrder:
		if item.State != "created" {
			return nil, fmt.Errorf("order, %v, has already shipped", command.AggregateID())
		}
		orderShipped := &OrderShipped{
			Model: eventsource.Model{ID: command.AggregateID(), Version: item.Version + 1, At: time.Now()},
		}
		return []eventsource.Event{orderShipped}, nil

	default:
		return nil, fmt.Errorf("unhandled command, %v", v)
	}
}

func TestSimpleScenario(t *testing.T) {
	scenario.New(t, &Order{}).
		Given().
		When(&CreateOrder{}).
		Then(&OrderCreated{})
}

type Errors struct {
	Messages []string
}

func (e *Errors) Errorf(format string, args ...interface{}) {
	e.Messages = append(e.Messages, fmt.Sprintf(format, args...))
}

func TestFieldError(t *testing.T) {
	errs := &Errors{}
	id := "abc"
	scenario.New(errs, &Order{}).
		Given().
		When(
			&CreateOrder{CommandModel: eventsource.CommandModel{ID: id}},
		).
		Then(
			&OrderCreated{Model: eventsource.Model{ID: id + "junk"}},
		)

	assert.Len(t, errs.Messages, 1)
	assert.True(t, strings.Contains(errs.Messages[0], "junk"))
}
