package pgstore_test

import (
	"database/sql"
	"testing"

	"fmt"
	"os"

	"github.com/altairsix/eventsource/pgstore"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

var (
	dsn string
)

func init() {
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	user := env("POSTGRES_TEST_USER", "altairsix")
	pass := env("POSTGRES_TEST_PASS", "password")
	host := env("POSTGRES_TEST_HOST", "localhost")
	port := env("POSTGRES_TEST_PORT", "5432")
	dbname := env("POSTGRES_TEST_DBNAME", "altairsix")
	dsn = fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v sslmode=disable",
		host, port, user, pass, dbname,
	)
}

type DB interface {
	pgstore.DB
}

func WithRollback(t *testing.T, fn func(db DB, tableName string)) {
	tableName := "sample"
	var db *sql.DB

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
