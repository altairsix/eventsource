package awscloud_test

import (
	"testing"

	"github.com/altairsix/eventsource/awscloud"
	"github.com/stretchr/testify/assert"
)

func TestDynamoDB(t *testing.T) {
	region := "the-region"
	endpoint := "http://my-endpoint"
	api, err := awscloud.DynamoDB(region, endpoint)
	assert.Nil(t, err)
	assert.Equal(t, endpoint, api.Endpoint)
	assert.Equal(t, endpoint, *api.Config.Endpoint)
	assert.Equal(t, region, *api.Config.Region)
}

func TestFirehose(t *testing.T) {
	region := "the-region"
	api, err := awscloud.Firehose(region)
	assert.Nil(t, err)
	assert.Equal(t, region, *api.Config.Region)
}
