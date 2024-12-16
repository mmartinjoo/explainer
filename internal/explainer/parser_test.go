package explainer

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestReadQueries(t *testing.T) {
	logs := []string{
		"[2024-12-13 20:06:25] local.INFO: select * from `page_views`",
		"[2024-12-13 20:06:50] local.INFO: select * from `sites`",
		"[2024-12-13 20:05:44] select * from `page_views` where `id` = ? [100]",
		"select * from `page_views` where `id` IN (?,?,?) [100,200,300]",
	}
	r := strings.NewReader(strings.Join(logs, "\n"))
	queries, err := readQueries(r)
	assert.Nil(t, err)
	assert.Equal(t, logs, queries)
}

func TestRejectWriteQueries(t *testing.T) {
	logs := []string{
		"[2024-12-13 20:06:25] local.INFO: select * from `page_views`",
		"insert into users(username, email) values(john.doe, john@doe.com)",
		"delete from product",
		"update categories set is_active=0 where id=10",
	}
	queries, err := rejectWriteQueries(logs)
	assert.Nil(t, err)
	assert.Len(t, queries, 1)
}

func TestSanitizeQueries(t *testing.T) {
	logs := []string{
		"[2024-12-13 20:06:25] local.INFO: select * from `page_views`   ",
		"other log line",
		"local.INFO: select * from `page_views` where id=? [10]",
		"select * from `page_views`",
	}
	queries, err := sanitizeQueries(logs)
	assert.Nil(t, err)
	assert.Len(t, queries, 3)

	assert.Equal(t, "select * from `page_views`", queries[0])
	assert.Equal(t, "select * from `page_views` where id=? [10]", queries[1])
	assert.Equal(t, "select * from `page_views`", queries[2])
}

func TestGetUniqueQueries(t *testing.T) {
	logs := []string{
		"select * from `page_views`",
		"select * from `page_views` where id=? [10]",
		"select * from `page_views` where id=? [15]",
		"select * from `page_views` where id IN (?,?) [10,15]",
	}
	queries, err := getUniqueQueries(logs)
	assert.Nil(t, err)
	assert.Len(t, queries, 3)

	assert.Contains(t, queries, "select * from `page_views`")
	assert.Contains(t, queries, "select * from `page_views` where id=? [15]")
	assert.Contains(t, queries, "select * from `page_views` where id IN (?,?) [10,15]")
}

func TestGetBindings(t *testing.T) {
	queries, err := getBindings("select * from `page_views` where id IN (?,?,?) [10,20,30]")
	assert.Nil(t, err)
	assert.Len(t, queries, 3)

	assert.Equal(t, "10", queries[0])
	assert.Equal(t, "20", queries[1])
	assert.Equal(t, "30", queries[2])
}

func TestGetBindings_NoBindings(t *testing.T) {
	_, err := getBindings("select * from `page_views`")
	assert.NotNil(t, err)
}
