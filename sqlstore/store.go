package sqlstore

import (
	"context"
	"database/sql"

	"strings"

	"github.com/altairsix/eventsource"
	"github.com/pkg/errors"
)

type Dialect string

const (
	MySQL      Dialect = "mysql"
	PostgreSQL Dialect = "postgres"
)

type Accessor interface {
	Open(ctx context.Context) (*sql.DB, error)
	Close(*sql.DB) error
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
			return errors.Wrap(err, "failed to insert record")
		}
	}

	return nil
}

// Load the history of events up to the version specified; when version is
// 0, all events will be loaded
func (s *Store) Load(ctx context.Context, aggregateID string, version int) (eventsource.History, error) {
	return nil, nil
}

func New(dialect Dialect, tableName string, opts ...Option) (*Store, error) {
	store := &Store{
		dialect:   dialect,
		tableName: tableName,
	}

	for _, opt := range opts {
		opt(store)
	}

	return store, nil
}
