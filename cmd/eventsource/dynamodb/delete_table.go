package dynamodb

import (
	"fmt"
	"log"
	"os"

	"github.com/altairsix/eventsource/awscloud"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"gopkg.in/urfave/cli.v1"
)

// DeleteTable holds the dynamodb delete-table command
var DeleteTable = cli.Command{
	Name:  "delete-table",
	Usage: "deletes the specified dynamodb table",
	Flags: []cli.Flag{
		flagName,
		flagRegion,
		flagEndpoint,
		flagDryrun,
	},
	Action: deleteTableAction,
}

func deleteTableAction(_ *cli.Context) error {
	w := os.Stdout

	api, err := awscloud.DynamoDB(opts.AWS.Region, opts.DynamoDB.Endpoint)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Fprintf(w, "Deleting table, %v.\n", opts.DynamoDB.TableName)
	_, err = api.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(opts.DynamoDB.TableName),
	})
	if err != nil {
		if v, ok := err.(awserr.Error); ok {
			if v.Code() == awsResourceNotFound {
				fmt.Fprintf(w, "Unable to delete table, %v.  Table not found.\n", opts.DynamoDB.TableName)
				return nil
			}
		}
		log.Fatalln(err)
	}

	fmt.Fprintf(w, "Successfully deleted table, %v.\n", opts.DynamoDB.TableName)

	return nil
}
