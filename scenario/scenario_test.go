package scenario_test

import (
	"fmt"
	"testing"
	"time"

	"context"

	"github.com/altairsix/eventsource"
	"github.com/altairsix/eventsource/scenario"
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
	return nil, nil
}

func TestSimpleScenario(t *testing.T) {
	scenario.New(t, &Order{}).
		Given().
		When(&CreateOrder{}).
		Then()
}
