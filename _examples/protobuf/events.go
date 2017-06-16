package main

import "time"

//go:generate protoc --go_out=. events.proto

// You can imagine these as auto generated machine code as well.
// Exercise left for the reader.

func (u *UserCreated) AggregateID() string {
	return u.GetAggregate().Id
}

func (u *UserCreated) EventVersion() int {
	return int(u.GetAggregate().Version)
}

func (u *UserCreated) EventAt() time.Time {
	return time.Unix(0, u.GetAggregate().At)
}

func (u *UserNameSet) AggregateID() string {
	return u.GetAggregate().Id
}

func (u *UserNameSet) EventVersion() int {
	return int(u.GetAggregate().Version)
}

func (u *UserNameSet) EventAt() time.Time {
	return time.Unix(0, u.GetAggregate().At)
}

func (u *UserEmailSet) AggregateID() string {
	return u.GetAggregate().Id
}

func (u *UserEmailSet) EventVersion() int {
	return int(u.GetAggregate().Version)
}

func (u *UserEmailSet) EventAt() time.Time {
	return time.Unix(0, u.GetAggregate().At)
}
