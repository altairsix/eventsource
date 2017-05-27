package sqlstore_test

import (
	"database/sql"
	"log"
	"testing"

	"github.com/altairsix/eventsource/sqlstore"
	"github.com/altairsix/eventsource/sqlstore/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func WithRollback(t *testing.T, dialect sqlstore.Dialect, fn func(db *sql.DB)) {
	tableName := "sample"
	var db *sql.DB

	switch dialect {
	case sqlstore.MySQL:
		v, err := sql.Open("mysql", "altairsix:password@tcp(localhost:3306)/altairsix?charset=utf8")
		if !assert.Nil(t, err, "unable to open connection") {
			return
		}
		db = v

		if err := mysql.CreateIfNotExists(db, tableName); err != nil {
			t.Errorf("unable to create table, %v", err)
			return
		}

	default:
		log.Fatalf("Unhandled database dialect, %v", dialect)
	}
	defer db.Close()

	tx, err := db.Begin()
	if !assert.Nil(t, err, "unable to begin transaction") {
		return
	}
	defer tx.Rollback()

	fn(db)
}

//
//func TempTable(t *testing.T, fn func(tableName string)) {
//	// Create a temporary table for use during this test
//	//
//	now := strconv.FormatInt(time.Now().UnixNano(), 36)
//	random := strconv.FormatInt(int64(r.Int31()), 36)
//	tableName := "tmp-" + now + "-" + random
//	input := dynamodbstore.MakeCreateTableInput(tableName, 50, 50)
//	_, err := api.CreateTable(input)
//	assert.Nil(t, err)
//	defer func() {
//		_, err := api.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
//		assert.Nil(t, err)
//	}()
//
//	fn(tableName)
//}
