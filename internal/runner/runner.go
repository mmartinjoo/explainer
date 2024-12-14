package runner

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/mmartinjoo/explainer/internal/platform"
)

func Run(db *sql.DB, queries []platform.Query) ([]platform.Explain, error) {
	res := make([]platform.Explain, 0)
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
			var explain platform.Explain
			explain.Query = q

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
			res = append(res, explain)
		} else {
			qErr := NewQueryError(q, fmt.Errorf("EXPLAIN returned an empty row"))
			log.Println(qErr)
			continue
		}
	}
	return res, nil
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
