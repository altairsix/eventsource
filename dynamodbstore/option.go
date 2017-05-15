package dynamodbstore

import (
	"io"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Option represents a functional configuration of *Store
type Option func(*Store)

// WithRegion specifies the AWS Region to connect to
func WithRegion(region string) Option {
	return func(s *Store) {
		s.region = region
	}
}

// WithEventPerItem allows you to specify the number of events to be stored per dynamodb record; defaults to 1
func WithEventPerItem(eventsPerItem int) Option {
	return func(s *Store) {
		s.eventsPerItem = eventsPerItem
	}
}

// WithDynamoDB allows the caller to specify a pre-configured reference to DynamoDB
func WithDynamoDB(api *dynamodb.DynamoDB) Option {
	return func(s *Store) {
		s.api = api
	}
}

// WithDebug provides additional debugging information
func WithDebug(w io.Writer) Option {
	return func(s *Store) {
		s.debug = true
		s.writer = w
	}
}
