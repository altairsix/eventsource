package boltdbstore

import (
	"context"
	"errors"
	"strconv"

	"fmt"

	"strings"

	"math"

	"bytes"

	"github.com/boltdb/bolt"
	"github.com/altairsix/eventsource"
)

const (
	delimeter = ":"
)

var (
	bucketKey        = []byte("eventsource")
	eventKeyTemplate = fmt.Sprintf("%%s:%s%%018d", delimeter)
)

//Store uses local BoltDB instance
type Store struct {
	db *bolt.DB
}

//New returns properly instanced Store with BoltDB backing
func New(databaseName string) (*Store, error) {
	fullPath := fmt.Sprintf("%s.bolt", databaseName)
	db, err := bolt.Open(fullPath, 0600, nil)
	if err != nil {
		return nil, err
	}
	s := Store{
		db: db,
	}
	return &s, nil
}

//Close should always be a defer call after New() instance
func (s *Store) Close() error {
	return s.db.Close()
}

// Save the provided serialized records to the store
func (s *Store) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	if strings.Contains(aggregateID, delimeter) {
		f := "can't include delimiter '%s' in aggregateID because used in underlying backing db."
		e := fmt.Sprintf(f, delimeter)
		return errors.New(e)
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketKey)
		if err != nil {
			return err
		}

		for _, r := range records {
			keyS := fmt.Sprintf(eventKeyTemplate, aggregateID, r.Version)
			key := []byte(keyS)
			value := r.Data
			b.Put(key, value)
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

	minS := fmt.Sprintf(eventKeyTemplate, aggregateID, fromVersion)
	min := []byte(minS)

	maxS := fmt.Sprintf(eventKeyTemplate, aggregateID, toVersion)
	max := []byte(maxS)

	history := make(eventsource.History, 0, eventCountGuess)
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketKey)
		if b == nil {
			return nil
		}

		c := b.Cursor()
		for key, data := c.Seek(min); key != nil && bytes.Compare(key, max) <= 0; key, data = c.Next() {
			versionRaw := key[18:]
			version, err := strconv.Atoi(string(versionRaw))

			if err != nil {
				return err
			}

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
