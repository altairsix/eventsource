package mysqlstore_test

import (
	"database/sql"
	"testing"

	"github.com/altairsix/eventsource/mysqlstore"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

type DB interface {
	mysqlstore.DB
}

func WithRollback(t *testing.T, fn func(db DB, tableName string)) {
	tableName := "sample"
	var db *sql.DB

	v, err := sql.Open("mysql", "altairsix:password@tcp(localhost:3306)/altairsix?charset=utf8")
	if !assert.Nil(t, err, "unable to open connection") {
		return
	}
	db = v

	if err := mysqlstore.CreateIfNotExists(db, tableName); err != nil {
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
