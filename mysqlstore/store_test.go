package mysqlstore_test

import (
	"context"
	"testing"

	"github.com/altairsix/eventsource"
	"github.com/altairsix/eventsource/mysqlstore"
	"github.com/stretchr/testify/assert"
)

type Accessor struct {
	db mysqlstore.DB
}

func (a Accessor) Open(ctx context.Context) (mysqlstore.DB, error) {
	return a.db, nil
}

func (a Accessor) Close(db mysqlstore.DB) error {
	return nil
}

func TestStore_ImplementsStore(t *testing.T) {
	v, err := mysqlstore.New("blah", nil)
	assert.Nil(t, err)

	var store eventsource.Store = v
	assert.NotNil(t, store)
}

func TestStore_ImplementsStreamReader(t *testing.T) {
	v, err := mysqlstore.New("blah", nil)
	assert.Nil(t, err)

	var reader eventsource.StreamReader = v
	assert.NotNil(t, reader)
}

func TestStore_SaveEmpty(t *testing.T) {
	s, err := mysqlstore.New("blah", nil)
	assert.Nil(t, err)

	err = s.Save(context.Background(), "abc")
	assert.Nil(t, err, "no records saved; guaranteed to work")
}

func TestStore_SaveAndFetch(t *testing.T) {
	WithRollback(t, func(db DB, tableName string) {
		ctx := context.Background()
		store, err := mysqlstore.New(tableName, Accessor{db: db})
		assert.Nil(t, err)

		aggregateID := "abc"
		history := eventsource.History{
			{
				Version: 1,
				Data:    []byte("a"),
			},
			{
				Version: 2,
				Data:    []byte("b"),
			},
			{
				Version: 3,
				Data:    []byte("c"),
			},
		}
		err = store.Save(ctx, aggregateID, history...)
		assert.Nil(t, err)

		found, err := store.Load(ctx, aggregateID, 0, 0)
		assert.Nil(t, err)
		assert.Equal(t, history, found)
		assert.Len(t, found, len(history))
	})
}

func TestStore_SaveAndRead(t *testing.T) {
	WithRollback(t, func(db DB, tableName string) {
		ctx := context.Background()
		store, err := mysqlstore.New(tableName, Accessor{db: db})
		assert.Nil(t, err)

		aggregateID := "abc"
		history := eventsource.History{
			{
				Version: 1,
				Data:    []byte("a"),
			},
			{
				Version: 2,
				Data:    []byte("b"),
			},
			{
				Version: 3,
				Data:    []byte("c"),
			},
		}
		err = store.Save(ctx, aggregateID, history...)
		assert.Nil(t, err)

		found, err := store.Read(ctx, 0, len(history))
		assert.Nil(t, err)
		assert.Len(t, found, len(history))

		for _, item := range found {
			assert.NotZero(t, item.Offset)
			assert.NotZero(t, item.AggregateID)
			assert.NotZero(t, item.Data)
			assert.NotZero(t, item.Version)
		}
	})
}

func TestStore_SaveIdempotent(t *testing.T) {
	WithRollback(t, func(db DB, tableName string) {
		ctx := context.Background()
		store, err := mysqlstore.New(tableName, Accessor{db: db})
		assert.Nil(t, err)

		aggregateID := "abc"
		history := eventsource.History{
			{
				Version: 1,
				Data:    []byte("a"),
			},
			{
				Version: 2,
				Data:    []byte("b"),
			},
			{
				Version: 3,
				Data:    []byte("c"),
			},
		}
		// initial save
		err = store.Save(ctx, aggregateID, history...)
		assert.Nil(t, err)

		// When - save it again
		err = store.Save(ctx, aggregateID, history...)
		// Then - verify no errors e.g. idempotent
		assert.Nil(t, err)

		found, err := store.Load(ctx, aggregateID, 0, 0)
		assert.Nil(t, err)
		assert.Equal(t, history, found)
		assert.Len(t, found, len(history))
	})
}

func TestStore_SaveOptimisticLock(t *testing.T) {
	ctx := context.Background()

	WithRollback(t, func(db DB, tableName string) {
		store, err := mysqlstore.New(tableName, Accessor{db: db})
		assert.Nil(t, err)

		aggregateID := "abc"
		initial := eventsource.History{
			{
				Version: 1,
				Data:    []byte("a"),
			},
			{
				Version: 2,
				Data:    []byte("b"),
			},
		}
		// initial save
		err = store.Save(ctx, aggregateID, initial...)
		assert.Nil(t, err)

		overlap := eventsource.History{
			{
				Version: 2,
				Data:    []byte("c"),
			},
			{
				Version: 3,
				Data:    []byte("d"),
			},
		}
		// save overlapping events; should not be allowed
		err = store.Save(ctx, aggregateID, overlap...)
		assert.NotNil(t, err)
	})
}

func TestStore_LoadPartition(t *testing.T) {
	WithRollback(t, func(db DB, tableName string) {
		store, err := mysqlstore.New(tableName, Accessor{db: db})
		assert.Nil(t, err)

		aggregateID := "abc"
		history := eventsource.History{
			{
				Version: 1,
				Data:    []byte("a"),
			},
			{
				Version: 2,
				Data:    []byte("b"),
			},
			{
				Version: 3,
				Data:    []byte("c"),
			},
		}
		ctx := context.Background()
		err = store.Save(ctx, aggregateID, history...)
		assert.Nil(t, err)

		found, err := store.Load(ctx, aggregateID, 0, 1)
		assert.Nil(t, err)
		assert.Len(t, found, 1)
		assert.Equal(t, history[0:1], found)
	})
}
