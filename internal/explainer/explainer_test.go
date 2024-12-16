package explainer

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAnalyzeAccessType_All(t *testing.T) {
	expl := ExplainResult{
		QueryType: sql.NullString{String: "ALL"},
	}
	res := newResult(expl)
	res = res.analyzeAccessType()

	assert.NotEmpty(t, res.accessTypeWarning)
	assert.Equal(t, float32(1), res.Grade())
}

func TestAnalyzeAccessType_IndexWithoutExtra(t *testing.T) {
	expl := ExplainResult{
		QueryType: sql.NullString{String: "Index"},
		Extra:     sql.NullString{String: ""},
	}
	res := newResult(expl)
	res = res.analyzeAccessType()

	assert.NotEmpty(t, res.accessTypeWarning)
	assert.Equal(t, float32(1), res.Grade())
}

func TestAnalyzeAccessType_IndexWithExtra(t *testing.T) {
	expl := ExplainResult{
		QueryType: sql.NullString{String: "Index"},
		Extra:     sql.NullString{String: "Using index"},
	}
	res := newResult(expl)
	res = res.analyzeAccessType()

	assert.NotEmpty(t, res.accessTypeWarning)
	assert.Equal(t, float32(2), res.Grade())
}

func TestAnalyzeAccessType_RangeWithoutExtra(t *testing.T) {
	expl := ExplainResult{
		QueryType: sql.NullString{String: "range"},
		Extra:     sql.NullString{String: ""},
	}
	res := newResult(expl)
	res = res.analyzeAccessType()

	assert.NotEmpty(t, res.accessTypeWarning)
	assert.Equal(t, float32(3), res.Grade())
}

func TestAnalyzeAccessType_RangeWithExtra(t *testing.T) {
	expl := ExplainResult{
		QueryType: sql.NullString{String: "range"},
		Extra:     sql.NullString{String: "Using index"},
	}
	res := newResult(expl)
	res = res.analyzeAccessType()

	assert.Empty(t, res.accessTypeWarning)
	assert.Equal(t, float32(4), res.Grade())
}

func TestAnalyzeAccessType_Const(t *testing.T) {
	expl := ExplainResult{
		QueryType: sql.NullString{String: "const"},
	}
	res := newResult(expl)
	res = res.analyzeAccessType()

	assert.Empty(t, res.accessTypeWarning)
	assert.Equal(t, float32(5), res.Grade())
}

func TestAnalyzeAccessType_Ref(t *testing.T) {
	expl := ExplainResult{
		QueryType: sql.NullString{String: "ref"},
	}
	res := newResult(expl)
	res = res.analyzeAccessType()

	assert.Empty(t, res.accessTypeWarning)
	assert.Equal(t, float32(5), res.Grade())
}

func TestAnalyzeFilteredRows_Low(t *testing.T) {
	expl := ExplainResult{
		Filtered: sql.NullFloat64{Float64: 45},
	}
	res := newResult(expl)
	res.grade = 5
	res = res.analyzeFilteredRows()

	assert.NotEmpty(t, res.filterWarning)
	assert.Equal(t, float32(4), res.Grade())
}

func TestAnalyzeFilteredRows_VeryLow(t *testing.T) {
	expl := ExplainResult{
		Filtered: sql.NullFloat64{Float64: 25},
	}
	res := newResult(expl)
	res.grade = 5
	res = res.analyzeFilteredRows()

	assert.NotEmpty(t, res.filterWarning)
	assert.Equal(t, float32(3), res.Grade())
}

func TestAnalyzeFileSort(t *testing.T) {
	expl := ExplainResult{
		Extra: sql.NullString{String: "Using filesort"},
	}
	res := newResult(expl)
	res.grade = 5
	res = res.analyzeFilesort()

	assert.NotEmpty(t, res.filesortWarning)
	assert.Equal(t, float32(4.5), res.Grade())
}

func TestAnalyzeSelectStar(t *testing.T) {
	expl := ExplainResult{
		Query: newQuery("select * from users"),
	}
	res := newResult(expl)
	res.grade = 5
	res = res.analyzeSelectStar()

	assert.NotEmpty(t, res.selectStarWarning)
	assert.Equal(t, float32(4.75), res.Grade())
}

func TestAnalyzeLikePattern(t *testing.T) {
	expl := ExplainResult{
		Query: newQueryWithBindings("select * from users where username LIKE ?", []string{"%jphn%"}),
	}
	res := newResult(expl)
	res.grade = 5
	res = res.analyzeLikePattern()

	assert.NotEmpty(t, res.likePatternWarning)
	assert.Equal(t, float32(4.5), res.Grade())
}

func TestAnalyzeSubqueryInSelect(t *testing.T) {
	expl := ExplainResult{
		Query: newQueryWithBindings("select users.id, (select count(*) from products) as c from users where username LIKE ?", []string{"%jphn%"}),
	}
	res := newResult(expl)
	res.grade = 5
	res = res.analyzeSubqueryInSelect()

	assert.NotEmpty(t, res.subqueryInSelectWarning)
	assert.Equal(t, float32(3), res.Grade())
}

func TestGetJoinedTables(t *testing.T) {
	sql := `
		select *
		from users
		join orders on orders.user_id = users.id
		join order_items on order_items.order_id = orders.id
	`
	assert.Equal(t, []string{"orders", "order_items"}, getJoinedTables(sql))
}

func TestGetJoinedTables_WithAlias(t *testing.T) {
	sql := `
		select *
		from users
		join orders as o on o.user_id = users.id
		join order_items as i on i.order_id = o.id
	`
	assert.Equal(t, []string{"orders", "order_items"}, getJoinedTables(sql))
}

func TestGetJoinedTables_WithAliasShort(t *testing.T) {
	sql := `
		select *
		from users
		join orders o on o.user_id = users.id
		join order_items i on i.order_id = o.id
	`
	assert.Equal(t, []string{"orders", "order_items"}, getJoinedTables(sql))
}
