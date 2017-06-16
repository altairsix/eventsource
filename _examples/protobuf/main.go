package main

import (
	"context"
	"fmt"
	"log"

	"github.com/altairsix/eventsource"
)

type User struct {
	ID      string
	Version int
	Name    string
	Email   string
}

func (item *User) On(event eventsource.Event) error {
	switch v := event.(type) {
	case *UserCreated:
		item.Version = v.EventVersion()
		item.ID = v.AggregateID()

	case *UserNameSet:
		item.Version = v.EventVersion()
		item.Name = v.Name

	case *UserEmailSet:
		item.Version = v.EventVersion()
		item.Email = v.Email

	default:
		return fmt.Errorf("unhandled event, %v", v)
	}

	return nil
}

func main() {
	serializer := eventsource.NewProtoSerializer()

	repo := eventsource.New(&User{},
		eventsource.WithSerializer(serializer),
	)

	id := "123"
	setNameEvent := &UserNameSet{
		Aggregate: &Aggregate{Id: id, Version: 1},
		Name:      "Joe Public",
	}
	setEmailEvent := &UserEmailSet{
		Aggregate: &Aggregate{Id: id, Version: 2},
		Email:     "joe.public@example.com",
	}

	ctx := context.Background()
	err := repo.Save(ctx, setEmailEvent, setNameEvent)
	if err != nil {
		log.Fatalln(err)
	}

	v, err := repo.Load(ctx, id)
	if err != nil {
		log.Fatalln(err)
	}

	user := v.(*User)
	fmt.Printf("Hello %v %v\n", user.Name, user.Email) // prints "Hello Joe Public joe.public@example.com"
}
