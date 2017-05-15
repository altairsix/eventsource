package dynamodbstore_test

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/altairsix/eventsource/awscloud"
	"github.com/altairsix/eventsource/dynamodbstore"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestMakeCreateTableInput(t *testing.T) {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	api, err := awscloud.DynamoDB(dynamodbstore.DefaultRegion, endpoint)
	assert.Nil(t, err)

	t.Run("default", func(t *testing.T) {
		tableName := "default-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		input := dynamodbstore.MakeCreateTableInput(tableName, 20, 30)

		_, err := api.CreateTable(input)
		assert.Nil(t, err)

		_, err = api.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		assert.Nil(t, err)
	})

	t.Run("kitchen-sink", func(t *testing.T) {
		rcap := int64(35)
		wcap := int64(25)

		tableName := "kitchen-sink-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		input := dynamodbstore.MakeCreateTableInput(tableName, rcap, wcap,
			dynamodbstore.WithRegion("us-west-2"),
			dynamodbstore.WithEventPerItem(200),
			dynamodbstore.WithDynamoDB(api),
			dynamodbstore.WithDebug(ioutil.Discard),
		)

		_, err := api.CreateTable(input)
		assert.Nil(t, err)

		out, err := api.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		assert.Nil(t, err)
		assert.Equal(t, rcap, *out.Table.ProvisionedThroughput.ReadCapacityUnits)
		assert.Equal(t, wcap, *out.Table.ProvisionedThroughput.WriteCapacityUnits)

		_, err = api.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		assert.Nil(t, err)
	})
}
