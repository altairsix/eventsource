package eventsource_test

import (
	"context"
	"testing"
	"time"

	"io/ioutil"

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
		return errors.New(eventsource.UnhandledEvent)
	}

	return nil
}

func TestNew(t *testing.T) {
	repository := eventsource.New(&Entity{})
	aggregate := repository.New()
	assert.NotNil(t, aggregate)
	assert.Equal(t, &Entity{}, aggregate)
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

		v, version, err := repository.Load(ctx, id)
		assert.Nil(t, err, "expected successful load")
		assert.Equal(t, 1, version)

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

		v, version, err = repository.Load(ctx, id)
		assert.Nil(t, err)
		assert.Equal(t, 2, version)

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

		v, _, err := registry.Load(ctx, id)
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

		v, _, err := registry.Load(ctx, id)
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

	v, _, err := registry.Load(ctx, id)
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
