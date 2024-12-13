package platform

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
