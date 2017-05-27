package pgstore_test

import (
	"database/sql"
	"testing"

	"github.com/altairsix/eventsource/pgstore"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

type DB interface {
	pgstore.DB
}

func WithRollback(t *testing.T, fn func(db DB, tableName string)) {
	tableName := "sample"
	var db *sql.DB

	dsn := "host=localhost port=5432 user=altairsix password=password dbname=altairsix sslmode=disable"
	v, err := sql.Open("postgres", dsn)
	if !assert.Nil(t, err, "unable to open connection") {
		return
	}
	db = v

	if err := pgstore.CreateIfNotExists(db, tableName); err != nil {
		t.Errorf("unable to create table, %v", err)
		return
	}
	defer db.Close()

	tx, err := db.Begin()
	if !assert.Nil(t, err, "unable to begin transaction") {
		return
	}
	defer tx.Rollback()

	fn(tx, tableName)
}
