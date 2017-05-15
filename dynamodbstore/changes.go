package dynamodbstore

import (
	"errors"
	"sort"
	"strings"

	"github.com/altairsix/eventsource"
	apex "github.com/apex/go-apex/dynamo"
)

// Changes returns an ordered list of changes from the *dynamodbstore.Record; will never return nil
func Changes(record *apex.Record) ([]eventsource.Record, error) {
	keys := map[string]struct{}{}

	// determine which keys are new

	if record != nil && record.Dynamodb != nil {
		if record.Dynamodb.NewImage != nil {
			for k := range record.Dynamodb.NewImage {
				if isKey(k) {
					keys[k] = struct{}{}
				}
			}
		}

		if record.Dynamodb.OldImage != nil {
			for k := range record.Dynamodb.OldImage {
				if isKey(k) {
					delete(keys, k)
				}
			}
		}
	}

	// using those keys, construct a sorted list of items

	items := make([]eventsource.Record, 0, len(keys))
	for key := range keys {
		version, err := versionFromKey(key)
		if err != nil {
			return nil, err
		}

		data := record.Dynamodb.NewImage[key].B

		items = append(items, eventsource.Record{
			Version: version,
			Data:    data,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Version < items[j].Version
	})

	return items, nil
}

var (
	errInvalidEventSource = errors.New("invalid event source arn")
)

// TableName extracts a table name from a dynamodb event source arn
// arn:aws:dynamodb:us-west-2:528688496454:table/table-local-orgs/stream/2017-03-14T04:49:34.930
func TableName(eventSource string) (string, error) {
	segments := strings.Split(eventSource, "/")
	if len(segments) < 2 {
		return "", errInvalidEventSource
	}

	return segments[1], nil
}
