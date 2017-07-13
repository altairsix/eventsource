package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/altairsix/eventsource"
	"github.com/altairsix/eventsource/dynamodbstore"
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

//Apply implements the CommandHandler interface
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

func check(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	store, err := dynamodbstore.New("orders",
		dynamodbstore.WithRegion("us-west-2"),
	)
	check(err)

	repo := eventsource.New(&Order{},
		eventsource.WithStore(store),
		eventsource.WithSerializer(eventsource.NewJSONSerializer(
			OrderCreated{},
			OrderShipped{},
		)),
	)

	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	ctx := context.Background()

	_, err = repo.Apply(ctx, &CreateOrder{
		CommandModel: eventsource.CommandModel{ID: id},
	})
	check(err)

	_, err = repo.Apply(ctx, &ShipOrder{
		CommandModel: eventsource.CommandModel{ID: id},
	})
	check(err)

	aggregate, err := repo.Load(ctx, id)
	check(err)

	found := aggregate.(*Order)
	fmt.Printf("Order %v [version %v] %v %v\n", found.ID, found.Version, found.State, found.UpdatedAt.Format(time.RFC822))
}
