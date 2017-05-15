package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/altairsix/eventsource"
)

// UserCreated defines a user creation event
type UserCreated struct {
	eventsource.Model
}

// UserFirstSet defines an event by simple struct embedding
type UserNameSet struct {
	eventsource.Model
	Name string
}

// UserLastSet defines an event via tags
type UserEmailSet struct {
	ID      string
	Version int
	At      time.Time
	Email   string
}

func (m UserEmailSet) AggregateID() string {
	return m.ID
}

func (m UserEmailSet) EventVersion() int {
	return m.Version
}

func (m UserEmailSet) EventAt() time.Time {
	return m.At
}

type User struct {
	ID      string
	Version int
	Name    string
	Email   string
}

func (item *User) On(event eventsource.Event) error {
	switch v := event.(type) {
	case *UserCreated:
		item.Version = v.Model.Version
		item.ID = v.Model.ID

	case *UserNameSet:
		item.Version = v.Model.Version
		item.Name = v.Name

	case *UserEmailSet:
		item.Version = v.Version
		item.Email = v.Email

	default:
		return fmt.Errorf("unhandled event, %v", v)
	}

	return nil
}

func main() {
	serializer := eventsource.NewJSONSerializer(
		UserCreated{},
		UserNameSet{},
		UserEmailSet{},
	)
	repo := eventsource.New(&User{},
		eventsource.WithSerializer(serializer),
	)

	id := "123"
	setNameEvent := &UserNameSet{
		Model: eventsource.Model{ID: id, Version: 1},
		Name:  "Joe Public",
	}
	setEmailEvent := &UserEmailSet{
		ID:      id,
		Version: 2,
		Email:   "joe.public@example.com",
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
