package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/altairsix/eventsource"
	"github.com/pkg/errors"
)

type Dialect string

const (
	MySQL      Dialect = "mysql"
	PostgreSQL Dialect = "postgres"
)

type DB interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

type Accessor interface {
	Open(ctx context.Context) (DB, error)
	Close(DB) error
}

type Store struct {
	dialect   Dialect
	tableName string
	accessor  Accessor
}

func (s *Store) expand(statement string) string {
	return strings.Replace(statement, "${TABLE}", s.tableName, -1)
}

// Save the provided serialized records to the store
func (s *Store) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	if len(records) == 0 {
		return nil
	}

	db, err := s.accessor.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "save failed; unable to connect to db")
	}
	defer s.accessor.Close(db)

	stmt, err := db.PrepareContext(ctx, s.expand(`INSERT INTO ${TABLE} (aggregate_id, data, version) VALUES (?, ?, ?)`))
	if err != nil {
		return errors.Wrap(err, "unable to prepare statement")
	}
	defer stmt.Close()

	for _, record := range records {
		_, err = stmt.Exec(aggregateID, record.Data, record.Version)
		if err != nil {
			return s.isIdempotent(ctx, aggregateID, records...)
		}
	}

	return nil
}

func (s *Store) isIdempotent(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	segments := eventsource.History(records)
	sort.Sort(segments)

	fromVersion := segments[0].Version
	toVersion := segments[len(segments)-1].Version
	loaded, err := s.doLoad(ctx, aggregateID, fromVersion, toVersion)
	if err != nil {
		return fmt.Errorf("unable to retrieve version %v-%v for aggregate, %v", fromVersion, toVersion, aggregateID)
	}

	if !reflect.DeepEqual(segments, loaded) {
		return fmt.Errorf("unable to save records; conflicting records detected for aggregate, %v", aggregateID)
	}

	return nil
}

// Load the history of events up to the version specified; when version is
// 0, all events will be loaded
func (s *Store) Load(ctx context.Context, aggregateID string, version int) (eventsource.History, error) {
	return s.doLoad(ctx, aggregateID, 0, version)
}

func (s *Store) doLoad(ctx context.Context, aggregateID string, initialVersion, version int) (eventsource.History, error) {
	db, err := s.accessor.Open(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "load failed; unable to connect to db")
	}
	defer s.accessor.Close(db)

	if version == 0 {
		version = math.MaxInt32
	}

	rows, err := db.Query(s.expand("SELECT data, version FROM ${TABLE} WHERE aggregate_id = ? AND version >= ? AND version <= ? ORDER BY version ASC"), aggregateID, initialVersion, version)
	if err != nil {
		return nil, errors.Wrap(err, "load failed; unable to query rows")
	}

	history := eventsource.History{}
	for rows.Next() {
		record := eventsource.Record{}
		if err := rows.Scan(&record.Data, &record.Version); err != nil {
			return nil, errors.Wrap(err, "load failed; unable to parse row")
		}
		history = append(history, record)
	}

	return history, nil
}

func New(dialect Dialect, tableName string, accessor Accessor, opts ...Option) (*Store, error) {
	store := &Store{
		dialect:   dialect,
		tableName: tableName,
		accessor:  accessor,
	}

	for _, opt := range opts {
		opt(store)
	}

	return store, nil
}
