package runner

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/mmartinjoo/explainer/internal/platform"
)

func Run(db *sql.DB, queries []platform.Query) ([]string, error) {
	for _, q := range queries {
		rows, err := db.Query(q.AsExplain(), q.Bindings...)
		if err != nil {
			qErr := NewQueryError(q, err)
			log.Println(qErr)
			continue
		}
		defer rows.Close()

		if err = rows.Err(); err != nil {
			qErr := NewQueryError(q, err)
			log.Println(qErr)
			continue
		}

		if rows.Next() {
			var explain Explain

			err = rows.Scan(
				&explain.ID,
				&explain.SelectType,
				&explain.Table,
				&explain.Partitions,
				&explain.QueryType,
				&explain.PossibleKeys,
				&explain.Key,
				&explain.KeyLen,
				&explain.Ref,
				&explain.NumberOfRows,
				&explain.Filtered,
				&explain.Extra,
			)
			if err != nil {
				qErr := NewQueryError(q, err)
				log.Println(qErr)
				continue
			}
			fmt.Printf("ID: %d, select_type: %s, table: %s, key: %s, extra: %s\n", explain.ID, explain.SelectType.String, explain.Table.String, explain.Key.String, explain.Extra.String)
		} else {
			qErr := NewQueryError(q, fmt.Errorf("EXPLAIN returned an empty row"))
			log.Println(qErr)
			continue
		}
	}
	return []string{}, nil
}

type Explain struct {
	ID           int
	SelectType   sql.NullString
	Table        sql.NullString
	Partitions   sql.NullString
	QueryType    sql.NullString
	PossibleKeys sql.NullString
	Key          sql.NullString
	KeyLen       sql.NullInt64
	Ref          sql.NullString
	NumberOfRows sql.NullInt64
	Filtered     sql.NullFloat64
	Extra        sql.NullString
}

type QueryError struct {
	sql      string
	bindings []any
	err      error
}

func (q QueryError) Error() string {
	return fmt.Sprintf("query %s with bindings %v failed: %v", q.sql, q.bindings, q.err)
}

func NewQueryError(q platform.Query, err error) QueryError {
	return QueryError{
		sql:      q.SQL,
		bindings: q.Bindings,
		err:      err,
	}
}
