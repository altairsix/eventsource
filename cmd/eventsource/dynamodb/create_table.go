package dynamodb

import (
	"fmt"
	"log"
	"os"

	"github.com/altairsix/eventsource/awscloud"
	"github.com/altairsix/eventsource/dynamodbstore"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"gopkg.in/urfave/cli.v1"
)

// CreateTable holds the dynamodb create-table command
var CreateTable = cli.Command{
	Name:  "create-table",
	Usage: "creates the specified dynamodb table",
	Flags: []cli.Flag{
		flagName,
		cli.Int64Flag{
			Name:        "wcap",
			Usage:       "write capacity",
			Value:       5,
			Destination: &opts.DynamoDB.WriteCapacity,
		},
		cli.Int64Flag{
			Name:        "rcap",
			Usage:       "read capacity",
			Value:       5,
			Destination: &opts.DynamoDB.ReadCapacity,
		},
		cli.IntFlag{
			Name:        "n",
			Usage:       "the number of events to store per partition",
			Value:       100,
			Destination: &opts.EventsPerItem,
		},
		flagRegion,
		flagEndpoint,
		flagDryrun,
	},
	Action: createTableAction,
}

func createTableAction(_ *cli.Context) error {
	w := os.Stdout

	api, err := awscloud.DynamoDB(opts.AWS.Region, opts.DynamoDB.Endpoint)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Fprintf(w, "Creating table, %v.\n", opts.DynamoDB.TableName)
	input := dynamodbstore.MakeCreateTableInput(
		opts.DynamoDB.TableName,
		opts.DynamoDB.ReadCapacity,
		opts.DynamoDB.WriteCapacity,
		dynamodbstore.WithRegion(opts.AWS.Region),
		dynamodbstore.WithEventPerItem(opts.EventsPerItem),
	)
	_, err = api.CreateTable(input)
	if err != nil {
		if v, ok := err.(awserr.Error); ok {
			if v.Code() == awsResourceInUse {
				fmt.Fprintf(w, "Table, %v, already exists or is being deleted.\n", opts.DynamoDB.TableName)
				return nil
			}
		}
		log.Fatalln(err)
	}

	fmt.Fprintf(w, "Successfully created table, %v.\n", opts.DynamoDB.TableName)

	return nil
}
