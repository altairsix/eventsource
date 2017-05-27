package sqlstore_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/altairsix/eventsource"
	"github.com/altairsix/eventsource/sqlstore"
	"github.com/stretchr/testify/assert"
)

func TestStore_ImplementsStore(t *testing.T) {
	v, err := sqlstore.New(sqlstore.MySQL, "blah")
	assert.Nil(t, err)

	var store eventsource.Store = v
	assert.NotNil(t, store)
}

func TestStore_SaveEmpty(t *testing.T) {
	s, err := sqlstore.New(sqlstore.MySQL, "blah")
	assert.Nil(t, err)

	err = s.Save(context.Background(), "abc")
	assert.Nil(t, err, "no records saved; guaranteed to work")
}

func TestFoo(t *testing.T) {
	WithRollback(t, sqlstore.MySQL, func(db *sql.DB) {
		row, err := db.Query("select 1")
		assert.Nil(t, err)
		defer row.Close()

		row.Next()
		i := 0
		err = row.Scan(&i)
		assert.Nil(t, err)
		assert.Equal(t, 1, i)
	})
}

//func TestStore_SaveAndFetch(t *testing.T) {
//	t.Parallel()
//
//	TempTable(t, api, func(tableName string) {
//		ctx := context.Background()
//		store, err := sqlstore.New(tableName)
//		assert.Nil(t, err)
//
//		aggregateID := "abc"
//		history := eventsource.History{
//			{
//				Version: 1,
//				Data:    []byte("a"),
//			},
//			{
//				Version: 2,
//				Data:    []byte("b"),
//			},
//			{
//				Version: 3,
//				Data:    []byte("c"),
//			},
//		}
//		err = store.Save(ctx, aggregateID, history...)
//		assert.Nil(t, err)
//
//		found, err := store.Load(ctx, aggregateID, 0)
//		assert.Nil(t, err)
//		assert.Equal(t, history, found)
//		assert.Len(t, found, len(history))
//	})
//}

//func TestStore_SaveIdempotent(t *testing.T) {
//	t.Parallel()
//
//	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
//	if endpoint == "" {
//		t.SkipNow()
//		return
//	}
//
//	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
//	assert.Nil(t, err)
//
//	TempTable(t, api, func(tableName string) {
//		ctx := context.Background()
//		store, err := dynamodbstore.New(tableName,
//			dynamodbstore.WithDynamoDB(api),
//		)
//		assert.Nil(t, err)
//
//		aggregateID := "abc"
//		history := eventsource.History{
//			{
//				Version: 1,
//				Data:    []byte("a"),
//			},
//			{
//				Version: 2,
//				Data:    []byte("b"),
//			},
//			{
//				Version: 3,
//				Data:    []byte("c"),
//			},
//		}
//		// initial save
//		err = store.Save(ctx, aggregateID, history...)
//		assert.Nil(t, err)
//
//		// When - save it again
//		err = store.Save(ctx, aggregateID, history...)
//		// Then - verify no errors e.g. idempotent
//		assert.Nil(t, err)
//
//		found, err := store.Load(ctx, aggregateID, 0)
//		assert.Nil(t, err)
//		assert.Equal(t, history, found)
//		assert.Len(t, found, len(history))
//	})
//}
//
//func TestStore_SaveOptimisticLock(t *testing.T) {
//	t.Parallel()
//	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
//	if endpoint == "" {
//		t.SkipNow()
//		return
//	}
//
//	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
//	assert.Nil(t, err)
//
//	TempTable(t, api, func(tableName string) {
//		ctx := context.Background()
//		store, err := dynamodbstore.New(tableName,
//			dynamodbstore.WithDynamoDB(api),
//		)
//		assert.Nil(t, err)
//
//		aggregateID := "abc"
//		initial := eventsource.History{
//			{
//				Version: 1,
//				Data:    []byte("a"),
//			},
//			{
//				Version: 2,
//				Data:    []byte("b"),
//			},
//		}
//		// initial save
//		err = store.Save(ctx, aggregateID, initial...)
//		assert.Nil(t, err)
//
//		overlap := eventsource.History{
//			{
//				Version: 2,
//				Data:    []byte("c"),
//			},
//			{
//				Version: 3,
//				Data:    []byte("d"),
//			},
//		}
//		// save overlapping events; should not be allowed
//		err = store.Save(ctx, aggregateID, overlap...)
//		assert.NotNil(t, err)
//	})
//}
//
//func TestStore_LoadPartition(t *testing.T) {
//	t.Parallel()
//	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
//	if endpoint == "" {
//		t.SkipNow()
//		return
//	}
//
//	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
//	assert.Nil(t, err)
//
//	TempTable(t, api, func(tableName string) {
//		store, err := dynamodbstore.New(tableName,
//			dynamodbstore.WithDynamoDB(api),
//			dynamodbstore.WithEventPerItem(2),
//		)
//		assert.Nil(t, err)
//
//		aggregateID := "abc"
//		history := eventsource.History{
//			{
//				Version: 1,
//				Data:    []byte("a"),
//			},
//			{
//				Version: 2,
//				Data:    []byte("b"),
//			},
//			{
//				Version: 3,
//				Data:    []byte("c"),
//			},
//		}
//		ctx := context.Background()
//		err = store.Save(ctx, aggregateID, history...)
//		assert.Nil(t, err)
//
//		found, err := store.Load(ctx, aggregateID, 1)
//		assert.Nil(t, err)
//		assert.Len(t, found, 1)
//		assert.Equal(t, history[0:1], found)
//	})
//}
