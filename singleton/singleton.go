package singleton

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/altairsix/eventsource"
	"github.com/altairsix/eventsource/awscloud"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	// DefaultRegion the region the singleton table will be located in by default
	DefaultRegion = "us-east-1"

	// HashKey is the hash key for the dynamodb table used by singleton
	HashKey = "key"

	// OwnerField is the field that holds the owner
	OwnerField = "owner"

	// ExpiresField is the field that holds the expires data
	ExpiresField = "expires"
)

const (
	ErrIsAlreadyReserved = "err:singleton:already_reserved"
)

// Option provides flexibility for configuring a singleton
type Option func(r *Registry)

// WithDynamoDB allows the caller to specify a pre-configured reference to DynamoDB
func WithDynamoDB(api *dynamodb.DynamoDB) Option {
	return func(r *Registry) {
		r.api = api
	}
}

// Resource represents the unique e
type Resource struct {
	// Type provides a namespace to allow multiple resources to be represented in the same table e.g. email
	Type string

	// ID is the unique constraint e.g. normalized email address
	ID string

	// Owner is an arbitrary string that identifies who is in possession of the resource.
	// The owner of the resource may call #Reserve any number of times.
	Owner string
}

// Key converts the Resource into the dynamodb hash key
func (r Resource) Key() string {
	return r.Type + ":" + r.ID
}

// Interface provides the interface that Commands must implement to be picked up
// by the singleton registry
type Interface interface {
	eventsource.Command
	Reserve() (Resource, time.Duration)
}

// Registry provides an API into the allocations that have been made
type Registry struct {
	tableName string
	region    string
	endpoint  string
	api       *dynamodb.DynamoDB
}

// record provides a struct representation of what is stored in dynamodb
type record struct {
	Key       string `dynamodbav:"key"`
	Owner     string `dynamodbav:"owner"`
	ExpiresAt int64  `dynamodbav:"expires"`
}

// IsAvailable indicates whether the resource is available to be reserved; nil indicate the
// resource is available
func (r *Registry) IsAvailable(ctx context.Context, resource Resource) error {
	out, err := r.api.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			HashKey: {S: aws.String(resource.Key())},
		},
	})
	if err != nil {
		return err
	}

	if len(out.Item) == 0 {
		// empty object
		return nil
	}

	item := &record{}
	err = dynamodbattribute.UnmarshalMap(out.Item, item)
	if err != nil {
		return err
	}

	if item.Owner != resource.Owner {
		return fmt.Errorf("not the owner")
	}

	if item.ExpiresAt < time.Now().Unix() {
		return fmt.Errorf("not found")
	}

	return nil
}

// Reserve the resource for the owner specified by the resource for the period specified
// If d == 0; then the reservation lasts forever
func (r *Registry) Reserve(ctx context.Context, resource Resource, d time.Duration) error {
	expiresAt := time.Now().Add(d).Unix()
	if d == 0 {
		expiresAt = math.MaxInt64
	}

	item, err := dynamodbattribute.MarshalMap(&record{
		Key:       resource.Key(),
		Owner:     resource.Owner,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		return err
	}

	_, err = r.api.PutItem(&dynamodb.PutItemInput{
		TableName:           aws.String(r.tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(#key) or #owner = :owner"),
		ExpressionAttributeNames: map[string]*string{
			"#key":   aws.String(HashKey),
			"#owner": aws.String(OwnerField),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":owner": {S: aws.String(resource.Owner)},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

// Release removes the reservation for an existing resource so it can be reserved
// again
func (r *Registry) Release(ctx context.Context, resource Resource) error {
	_, err := r.api.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			HashKey: {S: aws.String(resource.Key())},
		},
	})
	return err
}

// reserve determines whether the command is requesting a reservation and if it does, it performs the reservation
func (r *Registry) reserve(ctx context.Context, cmd eventsource.Command) error {
	v, ok := cmd.(Interface)
	if !ok {
		return nil
	}

	resource, duration := v.Reserve()
	err := r.Reserve(ctx, resource, duration)
	if err != nil {
		if v, ok := err.(awserr.Error); ok && v.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return eventsource.NewError(err, ErrIsAlreadyReserved, "%v resource already exists, %v", resource.Type, resource.ID)
		}
		return err
	}

	return nil
}

// Wrap wraps a dispatcher with the singleton handler and returns a new dispatcher.
// If any command implements singleton.Interface, the wrapped dispatcher will
// attempt to reserve the specified resource for
func (r *Registry) Wrap(dispatcher Dispatcher) Dispatcher {
	return DispatcherFunc(func(ctx context.Context, command eventsource.Command) error {
		if err := r.reserve(ctx, command); err != nil {
			return err
		}

		return dispatcher.Dispatch(ctx, command)
	})
}

// WrapRepository wraps an *eventsource.Repository and returns a new Repository that implements the Apply method.
// If any command implements singleton.Interface, the wrapped dispatcher will
// attempt to reserve the specified resource for
func (r *Registry) WrapRepository(repo Repository) Repository {
	return RepositoryFunc(func(ctx context.Context, command eventsource.Command) (int, error) {
		if err := r.reserve(ctx, command); err != nil {
			return 0, err
		}

		return repo.Apply(ctx, command)
	})
}

// IsAlreadyReserved returns true if the error indicates the resource already exists and is reserved by someone else
func IsAlreadyReserved(err error) bool {
	return eventsource.ErrHasCode(err, ErrIsAlreadyReserved)
}

// New constructs a new singleton registry to simplify access to resoure reservations
func New(tableName string, opts ...Option) (*Registry, error) {
	registry := &Registry{
		tableName: tableName,
		region:    DefaultRegion,
	}

	for _, opt := range opts {
		opt(registry)
	}

	if registry.api == nil {
		v, err := awscloud.DynamoDB(registry.region, registry.endpoint)
		if err != nil {
			return nil, err
		}
		registry.api = v
	}

	return registry, nil
}
