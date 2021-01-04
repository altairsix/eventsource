package boltdbstore

import (
	"context"
	"encoding/binary"
	"os"

	"fmt"

	"math"

	"bytes"

	"strings"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/vancelongwill/eventsource"
)

var (
	allEventsBucketKey  = []byte("allEventKeys")
	aggregatesBucketKey = []byte("aggregates")
)

//Store uses local BoltDB instance
type Store struct {
	filename string
	db       *bolt.DB
}

// uitob returns an 8-byte big endian representation of v.
func uitob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func btoi(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}

func btoui(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

//New returns properly instanced Store with BoltDB backing
func New(databaseName string) (*Store, error) {
	filename := fmt.Sprintf("%s.bolt", databaseName)
	db, err := bolt.Open(filename, 0600, nil)

	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(allEventsBucketKey)
		tx.CreateBucketIfNotExists(aggregatesBucketKey)
		return nil
	})

	if err != nil {
		return nil, err
	}
	s := Store{
		db:       db,
		filename: filename,
	}
	return &s, nil
}

//Delete closes DB then removes single DB file and lock
func (s *Store) Delete() {
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
	os.Remove(s.filename)
	os.Remove(s.filename + ".lock")
}

//Close should always be a defer call after New() instance
func (s *Store) Close() error {
	return s.db.Close()
}

// Save the provided serialized records to the store
func (s *Store) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	if len(records) == 0 {
		return nil
	}
	firstIncomingEventVersion := records[0].Version
	lastIncomingVersion := records[len(records)-1].Version

	err := s.db.Update(func(tx *bolt.Tx) error {
		allEventsBucket := tx.Bucket(allEventsBucketKey)
		allAggregatesBucket := tx.Bucket(aggregatesBucketKey)

		aggregateBucket, err := allAggregatesBucket.CreateBucketIfNotExists([]byte(aggregateID))
		if err != nil {
			return err
		}

		lastVersion := 0
		lastKey, _ := aggregateBucket.Cursor().Last()
		if lastKey != nil {
			lastVersion = btoi(lastKey)
		}

		if lastIncomingVersion == lastVersion {
			return nil
		}

		actualVersion := lastVersion + 1
		if firstIncomingEventVersion != actualVersion {
			return errors.Errorf("optimisitc lock failed. expected:%d actual:%d.", firstIncomingEventVersion, actualVersion)
		}

		for _, r := range records {
			allEventsOffset, err := allAggregatesBucket.NextSequence()
			if err != nil {
				return err
			}

			eventKey := itob(r.Version)
			allEventsKey := uitob(allEventsOffset)
			aggregateEventCombinedKey := append([]byte(aggregateID+" "), eventKey...)

			value := r.Data
			aggregateBucket.Put(eventKey, value)
			allEventsBucket.Put(allEventsKey, aggregateEventCombinedKey) //Index
		}
		return nil
	})

	return err
}

// Load the history of events up to the version specified.
// When toVersion is 0, all events will be loaded.
// To start at the beginning, fromVersion should be set to 0
func (s *Store) Load(ctx context.Context, aggregateID string, fromVersion, toVersion int) (eventsource.History, error) {
	eventCountGuess := toVersion - fromVersion
	if toVersion == 0 {
		toVersion = math.MaxInt64
		eventCountGuess = 128 //sane value
	}

	min := itob(fromVersion)
	max := itob(toVersion)
	history := make(eventsource.History, 0, eventCountGuess)

	err := s.db.View(func(tx *bolt.Tx) error {
		aggregatesBucket := tx.Bucket(aggregatesBucketKey)
		aggregateBucket := aggregatesBucket.Bucket([]byte(aggregateID))

		if aggregateBucket == nil {
			return nil //No events
		}

		c := aggregateBucket.Cursor()
		for key, data := c.Seek(min); key != nil && bytes.Compare(key, max) <= 0; key, data = c.Next() {
			version := btoi(key)

			e := eventsource.Record{
				Data:    data,
				Version: version,
			}
			history = append(history, e)
		}

		return nil
	})

	return history, err
}

//Read implements the StreamReader interface
func (s *Store) Read(ctx context.Context, startingOffset uint64, recordCount int) ([]eventsource.StreamRecord, error) {
	select {
	case <-ctx.Done():
		return nil, nil
	default:
		untilOffset := startingOffset + uint64(recordCount)

		eventCountGuess := recordCount
		if recordCount == 0 {
			untilOffset = math.MaxUint64
			eventCountGuess = 128 //sane value
		}

		history := make([]eventsource.StreamRecord, 0, eventCountGuess)

		min := uitob(startingOffset)
		max := uitob(untilOffset)

		s.db.View(func(tx *bolt.Tx) error {
			aggregatesBucket := tx.Bucket(aggregatesBucketKey)
			allEventsBucket := tx.Bucket(allEventsBucketKey)
			c := allEventsBucket.Cursor()

			for globalKey, aggregateEventKey := c.Seek(min); globalKey != nil && bytes.Compare(globalKey, max) <= 0; globalKey, aggregateEventKey = c.Next() {
				offset := btoui(globalKey)
				keyParts := strings.Split(string(aggregateEventKey), " ")
				aggregateID := keyParts[0]
				aggregateBucketKey := []byte(keyParts[1])
				version := btoi(aggregateBucketKey)

				b := aggregatesBucket.Bucket([]byte(aggregateID))
				data := b.Get(aggregateBucketKey)

				e := eventsource.StreamRecord{
					Record: eventsource.Record{
						Version: version,
						Data:    data,
					},
					Offset:      offset,
					AggregateID: aggregateID,
				}
				history = append(history, e)
			}

			return nil
		})

		return history, nil
	}
}
