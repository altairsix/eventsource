package dynamodbstore_test

import (
	"testing"

	"github.com/altairsix/eventsource"
	"github.com/altairsix/eventsource/dynamodbstore"
	apex "github.com/apex/go-apex/dynamo"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestRawEvents(t *testing.T) {
	testCases := map[string]struct {
		Record   *apex.Record
		Expected []eventsource.Record
	}{
		"simple": {
			Record: &apex.Record{
				Dynamodb: &apex.StreamRecord{
					NewImage: map[string]*dynamodb.AttributeValue{
						"_1": {B: []byte("a")},
						"_2": {B: []byte("b")},
						"_3": {B: []byte("c")},
					},
					OldImage: map[string]*dynamodb.AttributeValue{
						"_1": {B: []byte("a")},
					},
				},
			},
			Expected: []eventsource.Record{
				{
					Version: 2,
					Data:    []byte("b"),
				},
				{
					Version: 3,
					Data:    []byte("c"),
				},
			},
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			records, err := dynamodbstore.Changes(tc.Record)
			assert.Nil(t, err)
			assert.Len(t, records, 2)
			assert.Equal(t, tc.Expected, records)
		})
	}
}

func TestTableName(t *testing.T) {
	tableName, err := dynamodbstore.TableName("arn:aws:dynamodb:us-west-2:528688496454:table/table-local-orgs/stream/2017-03-14T04:49:34.930")
	assert.Nil(t, err)
	assert.Equal(t, "table-local-orgs", tableName)

	_, err = dynamodbstore.TableName("bogus")
	assert.NotNil(t, err)
}
