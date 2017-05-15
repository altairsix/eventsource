package main

import (
	"os"

	"github.com/altairsix/eventsource/cmd/eventsource/dynamodb"
	"github.com/altairsix/eventsource/cmd/eventsource/singleton"
	"gopkg.in/urfave/cli.v1"
)

func main() {
	app := cli.NewApp()
	app.Version = "0.1.0-SNAPSHOT"
	app.Usage = "Utilities for managing event source"
	app.Commands = []cli.Command{
		{
			Name:  "dynamodb",
			Usage: "manages DynamoDB backed event source",
			Subcommands: []cli.Command{
				dynamodb.CreateTable,
				dynamodb.DeleteTable,
			},
		},
		{
			Name:  "singleton",
			Usage: "manages the DynamoDB table for the singleton feature",
			Subcommands: []cli.Command{
				singleton.CreateTable,
				singleton.DeleteTable,
			},
		},
	}
	app.Run(os.Args)
}
