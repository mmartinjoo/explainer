package tableanalyzer

import (
	"database/sql"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCheckTooLongTextColumns(t *testing.T) {
	db := &sql.DB{}
	res := newResult()

	patches := gomonkey.ApplyFunc(queryTooLongTextColumns, func(db *sql.DB, table string) ([]TooLongTextColumn, error) {
		return []TooLongTextColumn{
			{col: Column{
				name:     "c1",
				dataType: "mediumtext",
				key:      "",
			}, maxLen: 16000000},
		}, nil
	})
	defer patches.Reset()

	err := res.checkTooLongTextColumns(db, "table")
	assert.Nil(t, err)
	assert.Equal(t, float32(4.75), res.grade)
	assert.NotNil(t, res.tooLongTextColumnsWarning)
}

func TestCheckStringIndexes(t *testing.T) {
	db := &sql.DB{}
	res := newResult()

	patches := gomonkey.ApplyFunc(queryStringColumns, func(db *sql.DB, table string) ([]Column, error) {
		return []Column{
			{name: "c1", dataType: "varchar(255)", key: "idx1"},
			{name: "c2", dataType: "mediumtext", key: "idx2"},
			{name: "c3", dataType: "text", key: "idx3"},
		}, nil
	})
	patches = gomonkey.ApplyFunc(queryIndexes, func(db *sql.DB, table string) ([]Index, error) {
		return []Index{
			{
				keyName:     "idx1",
				indexType:   "BTREE",
				seq:         1,
				column:      "c1",
				cardinality: 10,
			},
			{
				keyName:     "idx2",
				indexType:   "BTREE",
				seq:         1,
				column:      "c2",
				cardinality: 10,
			},
			{
				keyName:     "idx3",
				indexType:   "FULLTEXT",
				seq:         1,
				column:      "c3",
				cardinality: 10,
			},
		}, nil
	})
	defer patches.Reset()

	err := res.checkStringIndexes(db, "table")
	assert.Nil(t, err)
	assert.Equal(t, float32(4.5), res.grade)
	assert.NotNil(t, res.stringBasedIndexWarning)
	assert.Contains(t, res.stringBasedIndexWarning, "c1")
	assert.Contains(t, res.stringBasedIndexWarning, "c2")
	assert.NotContains(t, res.stringBasedIndexWarning, "c3")
}

func TestCheckCompositeIndexes(t *testing.T) {
	db := &sql.DB{}
	res := newResult()

	patches := gomonkey.ApplyFunc(queryIndexes, func(db *sql.DB, table string) ([]Index, error) {
		return []Index{
			{
				keyName:     "idx1",
				indexType:   "BTREE",
				seq:         1,
				column:      "c1",
				cardinality: 10,
			},
			{
				keyName:     "idx1",
				indexType:   "BTREE",
				seq:         2,
				column:      "c2",
				cardinality: 30,
			},
			{
				keyName:     "idx1",
				indexType:   "BTREE",
				seq:         3,
				column:      "c3",
				cardinality: 20,
			},
		}, nil
	})
	defer patches.Reset()

	err := res.checkCompositeIndexes(db, "table")
	assert.Nil(t, err)
	assert.Equal(t, float32(3), res.grade)
	assert.NotNil(t, res.compositeIndexWarnings)
}
