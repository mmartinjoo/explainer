package explainer

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

func runExplainQueries(db *sql.DB, queries []Query) ([]ExplainResult, error) {
	res := make([]ExplainResult, 0)
	for i, q := range queries {
		rows, err := db.Query(q.AsExplain(), q.Bindings...)
		if err != nil && strings.Contains(err.Error(), "Too many connections") {
			return res, newTooManyConnectionsError(i, q.SQL)
		}
		if err != nil {
			qErr := newQueryError(q, err)
			log.Println(qErr)
			continue
		}
		defer rows.Close()

		if err = rows.Err(); err != nil {
			qErr := newQueryError(q, err)
			log.Println(qErr)
			continue
		}

		if rows.Next() {
			var explain ExplainResult
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
				qErr := newQueryError(q, err)
				log.Println(qErr)
				continue
			}
			res = append(res, explain)
		} else {
			qErr := newQueryError(q, fmt.Errorf("EXPLAIN returned an empty row"))
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

func newQueryError(q Query, err error) QueryError {
	return QueryError{
		sql:      q.SQL,
		bindings: q.Bindings,
		err:      err,
	}
}

type TooManyConnectionsError struct {
	sql string
	idx int
}

func newTooManyConnectionsError(idx int, sql string) TooManyConnectionsError {
	return TooManyConnectionsError{
		idx: idx,
		sql: sql,
	}
}

func (e TooManyConnectionsError) Error() string {
	return fmt.Sprintf("database returned a 'Too many connections' error after %d queries. Please try again with a smaller log file or set a limit with the '--limit' option. last query: %s\n To increase the limit temporarily run: \"SET GLOBAL max_connections = 255;\"", e.idx, e.sql)
}
