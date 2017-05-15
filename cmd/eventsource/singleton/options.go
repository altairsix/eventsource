package singleton

import (
	"github.com/altairsix/eventsource/dynamodbstore"
	"gopkg.in/urfave/cli.v1"
)

const (
	awsResourceInUse    = "ResourceInUseException"
	awsResourceNotFound = "ResourceNotFoundException"
)

type options struct {
	EventsPerItem int
	Dryrun        bool
	AWS           struct {
		Region string
	}
	DynamoDB struct {
		Endpoint      string
		TableName     string
		ReadCapacity  int64
		WriteCapacity int64
		PartitionSize int
	}
}

var opts = options{}

var (
	flagName = cli.StringFlag{
		Name:        "name",
		Usage:       "name of the table",
		Destination: &opts.DynamoDB.TableName,
	}
	flagRegion = cli.StringFlag{
		Name:        "region",
		Usage:       "AWS region to place table in",
		Value:       dynamodbstore.DefaultRegion,
		EnvVar:      "AWS_DEFAULT_REGION",
		Destination: &opts.AWS.Region,
	}
	flagEndpoint = cli.StringFlag{
		Name:        "endpoint",
		Usage:       "specify the DynamoDB endpoint; useful for local testing",
		EnvVar:      "DYNAMODB_ENDPOINT",
		Destination: &opts.DynamoDB.Endpoint,
	}
	flagDryrun = cli.BoolFlag{
		Name:        "dryrun",
		Usage:       "perform the checks, but don't modify underlying infrastructure",
		Destination: &opts.Dryrun,
	}
)
