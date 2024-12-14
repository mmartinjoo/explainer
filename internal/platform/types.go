package platform

import (
	"database/sql"
	"strings"
)

type Query struct {
	SQL      string
	Bindings []any
}

func NewQuery(sql string) Query {
	return Query{
		SQL:      sql,
		Bindings: make([]any, 0),
	}
}

func NewQueryWithBindings(sql string, bindings []string) Query {
	b := make([]any, 0)
	for _, v := range bindings {
		b = append(b, v)
	}
	q := NewQuery(sql)
	q.Bindings = b
	return q
}

func (q Query) AsExplain() string {
	return "explain " + q.SQL
}

type Explain struct {
	Query        Query
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

func (e Explain) UsingIndex() bool {
	return strings.Contains(e.Extra.String, "Using index")
}

func (e Explain) UsingFilesort() bool {
	return strings.Contains(e.Extra.String, "Using filesort")
}

func (e Explain) UsingTemporary() bool {
	return strings.Contains(e.Extra.String, "Using temporary")
}
