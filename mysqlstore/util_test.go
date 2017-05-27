package mysqlstore_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/altairsix/eventsource/mysqlstore"
	_ "github.com/go-sql-driver/mysql"
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
	user := env("MYSQL_TEST_USER", "altairsix")
	name := env("MYSQL_TEST_DBNAME", "altairsix")
	pass := env("MYSQL_TEST_PASS", "password")
	protocol := env("MYSQL_TEST_PROT", "tcp")
	addr := env("MYSQL_TEST_ADDR", "localhost:3306")
	netAddr := fmt.Sprintf("%s(%s)", protocol, addr)
	dsn = fmt.Sprintf("%s:%s@%s/%s?charset=utf8", user, pass, netAddr, name)

	if os.Getenv("TRAVIS_BUILD_DIR") != "" {
		dsn = fmt.Sprintf("%s@/%s?charset=utf8", user, name)
	}
}

type DB interface {
	mysqlstore.DB
}

func WithRollback(t *testing.T, fn func(db DB, tableName string)) {
	tableName := "sample"
	var db *sql.DB

	v, err := sql.Open("mysql", dsn)
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
