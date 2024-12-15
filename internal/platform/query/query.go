package query

import (
	"database/sql"
	"errors"
	"fmt"
)

var (
	ErrEmptyResults = errors.New("empty results")
)

func CountRows(db *sql.DB, table string) (int, error) {
	rows, err := db.Query(fmt.Sprintf("select count(*) from %s", table))
	if err != nil {
		return -1, fmt.Errorf("query.CountRows: exeuting query: %w", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return -1, fmt.Errorf("query.CountRows: scanning count: %w", err)
		}
	}
	return count, nil
}
