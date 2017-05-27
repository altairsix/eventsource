package mysql

import (
	"database/sql"
	"strings"

	"github.com/pkg/errors"
)

const (
	CreateSQL = `
	CREATE TABLE IF NOT EXISTS ${TABLE} (
		id           INT PRIMARY KEY AUTO_INCREMENT,
		aggregate_id VARCHAR(255),
		data         VARBINARY(4096),
		version      INT
	) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;
`
	CheckIndexSQL = `
	SELECT
		COUNT(*) IndexIsThere
	FROM
		INFORMATION_SCHEMA.STATISTICS
	WHERE table_schema=DATABASE()
		AND table_name='${TABLE}' AND index_name='idx_${TABLE}';
`

	CreateIndexSQL = `
	CREATE UNIQUE INDEX idx_${TABLE}
	ON ${TABLE} (aggregate_id, version);
`
)

func expand(template, tableName string) string {
	return strings.Replace(template, `${TABLE}`, tableName, -1)
}

type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func CreateIfNotExists(db DB, tableName string) error {
	_, err := db.Exec(expand(CreateSQL, tableName))
	if err != nil {
		return errors.Wrap(err, "unable to create table")
	}

	row, err := db.Query(expand(CheckIndexSQL, tableName))
	if err != nil {
		return errors.Wrap(err, "query failed to determine if index exists")
	}

	row.Next()
	exists := 0
	err = row.Scan(&exists)
	if err != nil {
		return errors.Wrap(err, "unable to read response for whether index exists")
	}

	if exists > 0 {
		return nil
	}

	_, err = db.Exec(expand(CreateIndexSQL, tableName))
	if err != nil {
		return errors.Wrap(err, "unable to create index")
	}

	return err
}
