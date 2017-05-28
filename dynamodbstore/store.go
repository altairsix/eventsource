package dynamodbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"

	"github.com/altairsix/eventsource"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
)

const (
	// DefaultRegion is the region the dynamodb table is located int
	DefaultRegion = "us-east-1"

	// HashKey is the dynamodb hash key for the table; holds the aggregateID
	HashKey = "key"

	// RangeKey is the dynamodb range key for the table.  Single each dynamodb record
	// stores multiple events, you can think of the partition as the page number and
	// the number of event per record as the page size
	RangeKey = "partition"
)

var (
	errInvalidKey       = errors.New("invalid event key")
	errNoRecords        = errors.New("no records to save")
	errDuplicateVersion = errors.New("version numbers must be unique")
)

// Store represents a dynamodb backed eventsource.Store
type Store struct {
	region        string
	tableName     string
	hashKey       string
	rangeKey      string
	api           *dynamodb.DynamoDB
	eventsPerItem int
	debug         bool
	writer        io.Writer
}

// checkIdempotent will see if the specified records exist
func (s *Store) checkIdempotent(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	if len(records) == 0 {
		return nil
	}

	version := records[len(records)-1].Version
	history, err := s.Load(ctx, aggregateID, 0, version)
	if err != nil {
		return err
	}
	if len(history) < len(records) {
		return errors.New(awsConditionalCheckFailed)
	}

	recent := history[len(history)-len(records):]
	if !reflect.DeepEqual(recent, eventsource.History(records)) {
		return errors.New(awsConditionalCheckFailed)
	}

	return nil
}

// Save implements the eventsource.Store interface
func (s *Store) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	if len(records) == 0 {
		return nil
	}

	input, err := makeUpdateItemInput(s.tableName, s.hashKey, s.rangeKey, s.eventsPerItem, aggregateID, records...)
	if err != nil {
		return err
	}

	if s.debug {
		encoder := json.NewEncoder(s.writer)
		encoder.SetIndent("", "  ")
		encoder.Encode(input)
	}

	_, err = s.api.UpdateItem(input)
	if err != nil {
		if v, ok := err.(awserr.Error); ok {
			if v.Code() == awsConditionalCheckFailed {
				return s.checkIdempotent(ctx, aggregateID, records...)
			}
			return errors.Wrapf(err, "Save failed. %v [%v]", v.Message(), v.Code())
		}
		return err
	}

	return nil
}

// Load satisfies the Store interface and retrieve events from dynamodb
func (s *Store) Load(ctx context.Context, aggregateID string, fromVersion, toVersion int) (eventsource.History, error) {
	from := selectPartition(fromVersion, s.eventsPerItem)
	to := selectPartition(toVersion, s.eventsPerItem)
	input, err := makeQueryInput(s.tableName, s.hashKey, s.rangeKey, aggregateID, from, to)
	if err != nil {
		return nil, err
	}

	history := make(eventsource.History, 0, toVersion)

	var startKey map[string]*dynamodb.AttributeValue
	for {
		out, err := s.api.Query(input)
		if err != nil {
			return nil, err
		}

		if len(out.Items) == 0 {
			break
		}

		// events are stored within av as _t{version} = {event-type}, _d{version} = {serialized event}
		for _, item := range out.Items {
			for key, av := range item {
				if !isKey(key) {
					continue
				}

				recordVersion, err := versionFromKey(key)
				if err != nil {
					return nil, err
				}

				if recordVersion < fromVersion {
					continue
				}
				if toVersion > 0 && recordVersion > toVersion {
					continue
				}

				history = append(history, eventsource.Record{
					Version: recordVersion,
					Data:    av.B,
				})
			}
		}

		startKey = out.LastEvaluatedKey
		if len(startKey) == 0 {
			break
		}
	}

	sort.Slice(history, func(i, j int) bool {
		return history[i].Version < history[j].Version
	})

	return history, nil
}

// New constructs a new dynamodb backed store
func New(tableName string, opts ...Option) (*Store, error) {
	store := &Store{
		region:        DefaultRegion,
		tableName:     tableName,
		hashKey:       HashKey,
		rangeKey:      RangeKey,
		eventsPerItem: 100,
	}

	for _, opt := range opts {
		opt(store)
	}

	if store.api == nil {
		cfg := &aws.Config{Region: aws.String(store.region)}
		s, err := session.NewSession(cfg)
		if err != nil {
			if v, ok := err.(awserr.Error); ok {
				return nil, errors.Wrapf(err, "Unable to create AWS Session - %v [%v]", v.Message(), v.Code())
			}
			return nil, err
		}
		store.api = dynamodb.New(s)
	}

	return store, nil
}

func validateInput(records ...eventsource.Record) error {
	// must save at least one record
	if len(records) == 0 {
		return errNoRecords
	}

	// version numbers may not duplicated
	// TODO - do version numbers have to be sequential or is increasing satisfactory?
	for i := len(records) - 2; i >= 0; i-- {
		if records[i].Version == records[i+1].Version {
			return errDuplicateVersion
		}
	}

	return nil
}

func makeUpdateItemInput(tableName, hashKey, rangeKey string, eventsPerItem int, aggregateID string, records ...eventsource.Record) (*dynamodb.UpdateItemInput, error) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].Version < records[j].Version
	})

	err := validateInput(records...)
	if err != nil {
		return nil, err
	}

	partitionID := selectPartition(records[0].Version, eventsPerItem)

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			hashKey:  {S: aws.String(aggregateID)},
			rangeKey: {N: aws.String(strconv.Itoa(partitionID))},
		},
		ExpressionAttributeNames: map[string]*string{
			"#revision": aws.String("revision"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":one": {N: aws.String("1")},
		},
	}

	// Add each element within the partition to the UpdateItemInput

	condExpr := &bytes.Buffer{}
	updateExpr := &bytes.Buffer{}
	io.WriteString(updateExpr, "ADD #revision :one SET ")

	for index, record := range records {
		// Each event is store as two entries, an event entries and an event type entry.

		key := makeKey(record.Version)
		nameRef := "#" + key
		valueRef := ":" + key

		if index > 0 {
			io.WriteString(condExpr, " AND ")
			io.WriteString(updateExpr, ", ")
		}
		fmt.Fprintf(condExpr, "attribute_not_exists(%v)", nameRef)
		fmt.Fprintf(updateExpr, "%v = %v", nameRef, valueRef)
		input.ExpressionAttributeNames[nameRef] = aws.String(key)
		input.ExpressionAttributeValues[valueRef] = &dynamodb.AttributeValue{B: record.Data}
	}

	input.ConditionExpression = aws.String(condExpr.String())
	input.UpdateExpression = aws.String(updateExpr.String())

	return input, nil
}

// makeQueryInput
//  - partition - fetch up to this partition number; 0 to fetch all partitions
func makeQueryInput(tableName, hashKey, rangeKey string, aggregateID string, fromPartition, toPartition int) (*dynamodb.QueryInput, error) {
	input := &dynamodb.QueryInput{
		TableName:      aws.String(tableName),
		Select:         aws.String("ALL_ATTRIBUTES"),
		ConsistentRead: aws.Bool(true),
		ExpressionAttributeNames: map[string]*string{
			"#key": aws.String(hashKey),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":key": {S: aws.String(aggregateID)},
		},
	}

	if toPartition == 0 {
		input.KeyConditionExpression = aws.String("#key = :key")

	} else {
		input.KeyConditionExpression = aws.String("#key = :key AND #partition >= :from AND #partition <= :to")
		input.ExpressionAttributeNames["#partition"] = aws.String(rangeKey)
		input.ExpressionAttributeValues[":from"] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(fromPartition))}
		input.ExpressionAttributeValues[":to"] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(toPartition))}
	}

	return input, nil
}

func selectPartition(version, eventsPerItem int) int {
	return version / eventsPerItem
}
