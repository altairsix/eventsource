package dynamodbstore_test

import (
	"context"
	"os"
	"testing"

	"github.com/altairsix/eventsource"
	"github.com/altairsix/eventsource/awscloud"
	"github.com/altairsix/eventsource/dynamodbstore"
	"github.com/stretchr/testify/assert"
)

func TestStore_ImplementsStore(t *testing.T) {
	v, err := dynamodbstore.New("blah")
	assert.Nil(t, err)

	var store eventsource.Store = v
	assert.NotNil(t, store)
}

func TestStore_SaveEmpty(t *testing.T) {
	s, err := dynamodbstore.New("blah")
	assert.Nil(t, err)

	err = s.Save(context.Background(), "abc")
	assert.Nil(t, err, "no records saved; guaranteed to work")
}

func TestStore_SaveAndFetch(t *testing.T) {
	t.Parallel()

	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
	assert.Nil(t, err)

	TempTable(t, api, func(tableName string) {
		ctx := context.Background()
		store, err := dynamodbstore.New(tableName,
			dynamodbstore.WithDynamoDB(api),
		)
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

func TestStore_SaveAndLoadFromVersion(t *testing.T) {
	t.Parallel()

	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
	assert.Nil(t, err)

	TempTable(t, api, func(tableName string) {
		ctx := context.Background()
		store, err := dynamodbstore.New(tableName,
			dynamodbstore.WithDynamoDB(api),
		)
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

		found, err := store.Load(ctx, aggregateID, 2, 0)
		assert.Nil(t, err)
		assert.Equal(t, history[1:], found)
		assert.Len(t, found, len(history)-1)
	})
}

func TestStore_SaveIdempotent(t *testing.T) {
	t.Parallel()

	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
	assert.Nil(t, err)

	TempTable(t, api, func(tableName string) {
		ctx := context.Background()
		store, err := dynamodbstore.New(tableName,
			dynamodbstore.WithDynamoDB(api),
		)
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
	t.Parallel()
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
	assert.Nil(t, err)

	TempTable(t, api, func(tableName string) {
		ctx := context.Background()
		store, err := dynamodbstore.New(tableName,
			dynamodbstore.WithDynamoDB(api),
		)
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
	t.Parallel()
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
	assert.Nil(t, err)

	TempTable(t, api, func(tableName string) {
		store, err := dynamodbstore.New(tableName,
			dynamodbstore.WithDynamoDB(api),
			dynamodbstore.WithEventPerItem(2),
		)
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
