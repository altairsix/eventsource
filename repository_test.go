package eventsource_test

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/altairsix/eventsource"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

type Entity struct {
	Version   int
	ID        string
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type EntityCreated struct {
	eventsource.Model
}

type EntityNameSet struct {
	eventsource.Model
	Name string
}

func (item *Entity) On(event eventsource.Event) error {
	switch v := event.(type) {
	case *EntityCreated:
		item.Version = v.Model.Version
		item.ID = v.Model.ID
		item.CreatedAt = v.Model.At
		item.UpdatedAt = v.Model.At

	case *EntityNameSet:
		item.Version = v.Model.Version
		item.Name = v.Name
		item.UpdatedAt = v.Model.At

	default:
		return errors.New(eventsource.ErrUnhandledEvent)
	}

	return nil
}

type CreateEntity struct {
	eventsource.CommandModel
}

type Nop struct {
	eventsource.CommandModel
}

func (item *Entity) Apply(ctx context.Context, command eventsource.Command) ([]eventsource.Event, error) {
	switch command.(type) {
	case *CreateEntity:
		return []eventsource.Event{&EntityCreated{
			Model: eventsource.Model{
				ID:      command.AggregateID(),
				Version: item.Version + 1,
				At:      time.Now(),
			},
		}}, nil

	case *Nop:
		return []eventsource.Event{}, nil

	default:
		return []eventsource.Event{}, nil
	}
}

func TestNew(t *testing.T) {
	repository := eventsource.New(&Entity{})
	aggregate := repository.New()
	assert.NotNil(t, aggregate)
	assert.Equal(t, &Entity{}, aggregate)
}

func TestRepository_Load_NotFound(t *testing.T) {
	ctx := context.Background()
	repository := eventsource.New(&Entity{},
		eventsource.WithDebug(ioutil.Discard),
	)

	_, err := repository.Load(ctx, "does-not-exist")
	assert.NotNil(t, err)
	assert.True(t, eventsource.IsNotFound(err))
}

func TestRegistry(t *testing.T) {
	ctx := context.Background()
	id := "123"
	name := "Jones"
	serializer := eventsource.NewJSONSerializer(
		EntityCreated{},
		EntityNameSet{},
	)

	t.Run("simple", func(t *testing.T) {
		repository := eventsource.New(&Entity{},
			eventsource.WithSerializer(serializer),
			eventsource.WithDebug(ioutil.Discard),
		)

		// Test - Add an event to the store and verify we can recreate the object

		err := repository.Save(ctx,
			&EntityCreated{
				Model: eventsource.Model{ID: id, Version: 0, At: time.Unix(3, 0)},
			},
			&EntityNameSet{
				Model: eventsource.Model{ID: id, Version: 1, At: time.Unix(4, 0)},
				Name:  name,
			},
		)
		assert.Nil(t, err)

		v, err := repository.Load(ctx, id)
		assert.Nil(t, err, "expected successful load")

		org, ok := v.(*Entity)
		assert.True(t, ok)
		assert.Equal(t, id, org.ID, "expected restored id")
		assert.Equal(t, name, org.Name, "expected restored name")

		// Test - Update the org name and verify that the change is reflected in the loaded result

		updated := "Sarah"
		err = repository.Save(ctx, &EntityNameSet{
			Model: eventsource.Model{ID: id, Version: 2},
			Name:  updated,
		})
		assert.Nil(t, err)

		v, err = repository.Load(ctx, id)
		assert.Nil(t, err)

		org, ok = v.(*Entity)
		assert.True(t, ok)
		assert.Equal(t, id, org.ID)
		assert.Equal(t, updated, org.Name)
	})

	t.Run("with pointer prototype", func(t *testing.T) {
		registry := eventsource.New(&Entity{},
			eventsource.WithSerializer(serializer),
		)

		err := registry.Save(ctx,
			&EntityCreated{
				Model: eventsource.Model{ID: id, Version: 0, At: time.Unix(3, 0)},
			},
			&EntityNameSet{
				Model: eventsource.Model{ID: id, Version: 1, At: time.Unix(4, 0)},
				Name:  name,
			},
		)
		assert.Nil(t, err)

		v, err := registry.Load(ctx, id)
		assert.Nil(t, err)
		assert.Equal(t, name, v.(*Entity).Name)
	})

	t.Run("with pointer bind", func(t *testing.T) {
		registry := eventsource.New(&Entity{},
			eventsource.WithSerializer(serializer),
		)

		err := registry.Save(ctx,
			&EntityNameSet{
				Model: eventsource.Model{ID: id, Version: 0},
				Name:  name,
			},
		)
		assert.Nil(t, err)

		v, err := registry.Load(ctx, id)
		assert.Nil(t, err)
		assert.Equal(t, name, v.(*Entity).Name)
	})
}

func TestAt(t *testing.T) {
	ctx := context.Background()
	id := "123"

	registry := eventsource.New(&Entity{},
		eventsource.WithSerializer(eventsource.NewJSONSerializer(EntityCreated{})),
	)

	err := registry.Save(ctx,
		&EntityCreated{
			Model: eventsource.Model{ID: id, Version: 1, At: time.Now()},
		},
	)
	assert.Nil(t, err)

	v, err := registry.Load(ctx, id)
	assert.Nil(t, err)

	org := v.(*Entity)
	assert.NotZero(t, org.CreatedAt)
	assert.NotZero(t, org.UpdatedAt)
}

func TestRepository_SaveNoEvents(t *testing.T) {
	repository := eventsource.New(&Entity{})
	err := repository.Save(context.Background())
	assert.Nil(t, err)
}

func TestWithObservers(t *testing.T) {
	captured := []eventsource.Event{}
	observer := func(event eventsource.Event) {
		captured = append(captured, event)
	}

	repository := eventsource.New(&Entity{},
		eventsource.WithSerializer(
			eventsource.NewJSONSerializer(
				EntityCreated{},
				EntityNameSet{},
			),
		),
		eventsource.WithDebug(ioutil.Discard),
		eventsource.WithObservers(observer),
	)

	ctx := context.Background()

	// When I dispatch command
	err := repository.Dispatch(ctx, &CreateEntity{
		CommandModel: eventsource.CommandModel{ID: "abc"},
	})

	// Then I expect event to be captured
	assert.Nil(t, err)
	assert.Len(t, captured, 1)

	_, ok := captured[0].(*EntityCreated)
	assert.True(t, ok)
}

func TestApply(t *testing.T) {
	repo := eventsource.New(&Entity{},
		eventsource.WithSerializer(
			eventsource.NewJSONSerializer(
				EntityCreated{},
			),
		),
	)

	cmd := &CreateEntity{CommandModel: eventsource.CommandModel{ID: "123"}}

	// When
	version, err := repo.Apply(context.Background(), cmd)

	// Then
	assert.Nil(t, err)
	assert.Equal(t, 1, version)

	// And
	version, err = repo.Apply(context.Background(), cmd)

	// Then
	assert.Nil(t, err)
	assert.Equal(t, 2, version)
}

func TestApplyNopCommand(t *testing.T) {
	t.Run("Version still returned when command generates no events", func(t *testing.T) {
		repo := eventsource.New(&Entity{},
			eventsource.WithSerializer(
				eventsource.NewJSONSerializer(
					EntityCreated{},
				),
			),
		)

		cmd := &Nop{
			CommandModel: eventsource.CommandModel{ID: "abc"},
		}
		version, err := repo.Apply(context.Background(), cmd)
		assert.Nil(t, err)
		assert.Equal(t, 0, version)
	})
}
