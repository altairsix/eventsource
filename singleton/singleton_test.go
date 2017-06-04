package singleton_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/altairsix/eventsource"
	"github.com/altairsix/eventsource/awscloud"
	"github.com/altairsix/eventsource/dynamodbstore"
	"github.com/altairsix/eventsource/singleton"
	"github.com/stretchr/testify/assert"
)

func TestRegistry_Lifecycle(t *testing.T) {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
	assert.Nil(t, err)

	ctx := context.Background()
	resource := singleton.Resource{
		Type:  "email",
		ID:    "id",
		Owner: "abc",
	}
	other := singleton.Resource{
		Type:  resource.Type,
		ID:    resource.ID,
		Owner: resource.Owner + "blah",
	}

	TempTable(t, api, func(tableName string) {
		registry, err := singleton.New(tableName,
			singleton.WithDynamoDB(api),
		)
		assert.Nil(t, err)

		// Should be available, no one's allocated it
		err = registry.IsAvailable(ctx, resource)
		assert.Nil(t, err)

		// Reserve it
		err = registry.Reserve(ctx, resource, time.Hour)
		assert.Nil(t, err)

		// Owner should show it as available
		err = registry.IsAvailable(ctx, resource)
		assert.Nil(t, err)

		// But others will see it as occupied
		err = registry.IsAvailable(ctx, other)
		assert.NotNil(t, err)

		// However, once we release it
		err = registry.Release(ctx, resource)
		assert.Nil(t, err)

		// Others may see it as available
		err = registry.IsAvailable(ctx, other)
		assert.Nil(t, err)
	})
}

func TestRegistry_ReleaseIdempotent(t *testing.T) {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
	assert.Nil(t, err)

	ctx := context.Background()
	resource := singleton.Resource{
		Type:  "email",
		ID:    "id",
		Owner: "abc",
	}

	TempTable(t, api, func(tableName string) {
		registry, err := singleton.New(tableName,
			singleton.WithDynamoDB(api),
		)
		assert.Nil(t, err)

		// Reserve it
		err = registry.Reserve(ctx, resource, time.Hour)
		assert.Nil(t, err)

		// However, once we release it
		err = registry.Release(ctx, resource)
		assert.Nil(t, err)

		// However, once we release it
		err = registry.Release(ctx, resource)
		assert.Nil(t, err)
	})
}

func TestRegistry_AllocateIdempotent(t *testing.T) {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
	assert.Nil(t, err)

	ctx := context.Background()
	resource := singleton.Resource{
		Type:  "email",
		ID:    "id",
		Owner: "abc",
	}

	TempTable(t, api, func(tableName string) {
		registry, err := singleton.New(tableName,
			singleton.WithDynamoDB(api),
		)
		assert.Nil(t, err)

		// Reserve it
		err = registry.Reserve(ctx, resource, time.Hour)
		assert.Nil(t, err)

		// Reserve it
		err = registry.Reserve(ctx, resource, time.Hour)
		assert.Nil(t, err)
	})
}

func TestRegistry_Wrap(t *testing.T) {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
	assert.Nil(t, err)

	ctx := context.Background()
	resource := singleton.Resource{
		Type:  "email",
		ID:    "id",
		Owner: "user-1",
	}

	TempTable(t, api, func(tableName string) {
		registry, err := singleton.New(tableName,
			singleton.WithDynamoDB(api),
		)
		assert.Nil(t, err)

		// user-1 allocates it
		err = registry.Reserve(ctx, resource, time.Hour)
		assert.Nil(t, err)

		fn := singleton.DispatcherFunc(func(ctx context.Context, command eventsource.Command) error {
			return nil
		})
		dispatcher := registry.Wrap(fn)

		// the original allocator can dispatch the command
		err = dispatcher.Dispatch(ctx, Command{
			ID:    resource.ID,
			Owner: resource.Owner,
		})
		assert.Nil(t, err)

		// but another user cannot
		err = dispatcher.Dispatch(ctx, Command{
			ID:    resource.ID,
			Owner: resource.Owner + "blah",
		})
		assert.NotNil(t, err)
		assert.True(t, singleton.IsAlreadyReserved(err))
	})
}

type Command struct {
	eventsource.CommandModel
	ID    string
	Owner string
}

func (c Command) Reserve() (singleton.Resource, time.Duration) {
	return singleton.Resource{
		Type:  "email",
		ID:    c.ID,
		Owner: c.Owner,
	}, time.Hour
}
