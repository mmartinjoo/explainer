package tableanalyzer

import (
	"database/sql"
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAnalyzeTooLongTextColumns(t *testing.T) {
	db := &sql.DB{}
	res := newResult()

	patches := gomonkey.ApplyFunc(queryTooLongTextColumns, func(db *sql.DB, table string) ([]TooLongTextColumn, error) {
		fmt.Printf("------- Mock function --------\n")
		return []TooLongTextColumn{
			{col: Column{
				name:     "c1",
				dataType: "mediumtext",
				key:      "",
			}, maxLen: 16000000},
		}, nil
	})
	defer patches.Reset()

	err := res.analyzeTooLongTextColumns(db, "table")
	assert.Nil(t, err)
	assert.Equal(t, float32(4.75), res.grade)
	assert.NotNil(t, res.tooLongTextColumnsWarning)
}
