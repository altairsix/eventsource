package singleton

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// MakeCreateTableInput is a utility tool to write the default table definition for creating the aws tables
func MakeCreateTableInput(tableName string, readCapacity, writeCapacity int64, opts ...Option) *dynamodb.CreateTableInput {
	registry := &Registry{
		region:    DefaultRegion,
		tableName: tableName,
	}

	for _, opt := range opts {
		opt(registry)
	}

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(HashKey),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(HashKey),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(readCapacity),
			WriteCapacityUnits: aws.Int64(writeCapacity),
		},
	}

	return input
}
