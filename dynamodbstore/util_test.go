package dynamodbstore_test

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/altairsix/eventsource/dynamodbstore"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

var (
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func TempTable(t *testing.T, api *dynamodb.DynamoDB, fn func(tableName string)) {
	// Create a temporary table for use during this test
	//
	now := strconv.FormatInt(time.Now().UnixNano(), 36)
	random := strconv.FormatInt(int64(r.Int31()), 36)
	tableName := "tmp-" + now + "-" + random
	input := dynamodbstore.MakeCreateTableInput(tableName, 50, 50)
	_, err := api.CreateTable(input)
	assert.Nil(t, err)
	defer func() {
		_, err := api.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
		assert.Nil(t, err)
	}()

	fn(tableName)
}
