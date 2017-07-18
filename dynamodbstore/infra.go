package dynamodbstore

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// MakeCreateTableInput is a utility tool to write the default table definition for creating the aws tables
func MakeCreateTableInput(tableName string, readCapacity, writeCapacity int64, opts ...Option) *dynamodb.CreateTableInput {
	store := &Store{
		region:    DefaultRegion,
		tableName: tableName,
		hashKey:   HashKey,
		rangeKey:  RangeKey,
	}

	for _, opt := range opts {
		opt(store)
	}

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(store.hashKey),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String(store.rangeKey),
				AttributeType: aws.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(store.hashKey),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(store.rangeKey),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(readCapacity),
			WriteCapacityUnits: aws.Int64(writeCapacity),
		},
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String("NEW_AND_OLD_IMAGES"),
		},
	}

	return input
}
