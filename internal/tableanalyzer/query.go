package tableanalyzer

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/mmartinjoo/explainer/internal/platform"
	"slices"
	"strings"
)

type TooLongTextColumn struct {
	col    Column
	maxLen int
}

func newTooLongTextColumn(col Column, maxLen int) TooLongTextColumn {
	return TooLongTextColumn{
		col:    col,
		maxLen: maxLen,
	}
}

// queryStringColumns returns varchar, mediumtext, text, etc columns from a table
func queryStringColumns(db *sql.DB, table string) ([]Column, error) {
	rows, err := db.Query(fmt.Sprintf("show columns from %s", table))
	if err != nil {
		return nil, fmt.Errorf("analyzer.queryStringColumns: exeuting query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("analyzer.queryStringColumns: reading columns: %w", err)
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))

	columns := make([]Column, 0)
	for rows.Next() {
		for i := range cols {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("analyzer.queryStringColumns: scanning rows: %w", err)
		}

		var column Column
		name, err := platform.ConvertString(values[0])
		if err != nil {
			return nil, fmt.Errorf("analyzer.queryStringColumns: parsing name: %w", err)
		}
		column.name = name

		dataType, err := platform.ConvertString(values[1])
		if err != nil {
			return nil, fmt.Errorf("analyzer.queryStringColumns: parsing dataType: %w", err)
		}
		column.dataType = dataType

		key, err := platform.ConvertString(values[3])
		if err != nil {
			return nil, fmt.Errorf("analyzer.queryStringColumns: parsing key: %w", err)
		}
		column.key = key

		if strings.Contains(column.dataType, "varchar") {
			columns = append(columns, column)
			continue
		}

		if slices.Contains([]string{"tinytext", "mediumtext", "longtext"}, column.dataType) {
			columns = append(columns, column)
			continue
		}
	}
	return columns, nil
}

func queryIndexes(db *sql.DB, table string) ([]Index, error) {
	rows, err := db.Query(fmt.Sprintf("show index from %s", table))
	if err != nil {
		return nil, fmt.Errorf("analyzer.queryIndexes: exeuting query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("analyzer.queryIndexes: reading columns: %w", err)
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))

	indexes := make([]Index, 0)
	for rows.Next() {
		for i := range cols {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("analyzer.queryIndexes: scanning rows: %w", err)
		}

		var idx Index
		key, err := platform.ConvertString(values[2])
		if err != nil {
			return nil, fmt.Errorf("analyzer.queryIndexes: parsing key: %w", err)
		}
		idx.keyName = key

		col, err := platform.ConvertString(values[4])
		if err != nil {
			return nil, fmt.Errorf("analyzer.queryIndexes: parsing col: %w", err)
		}
		idx.column = col

		idxType, err := platform.ConvertString(values[10])
		if err != nil {
			return nil, fmt.Errorf("analyzer.queryIndexes: parsing idxType: %w", err)
		}
		idx.indexType = idxType

		seq, ok := values[3].(int64)
		if !ok {
			return nil, fmt.Errorf("analyzer.queryIndexes: parsing sequence: %w", err)
		}
		idx.seq = seq

		card, ok := values[6].(int64)
		if !ok {
			return nil, fmt.Errorf("analyzer.queryIndexes: parsing cardinality: %w", err)
		}
		idx.cardinality = card

		indexes = append(indexes, idx)
	}
	return indexes, nil
}

func queryTooLongTextColumns(db *sql.DB, table string) ([]TooLongTextColumn, error) {
	stringCols, err := queryStringColumns(db, table)
	if err != nil {
		return nil, fmt.Errorf("abalyzer.queryTooLongTextColumns: querying columns: %w", err)
	}

	longTextCols := make([]Column, 0)
	for _, c := range stringCols {
		if c.dataType == "longtext" {
			longTextCols = append(longTextCols, c)
		}
	}

	res := make([]TooLongTextColumn, 0)
	for _, c := range longTextCols {
		maxLen, err := queryMaxLen(db, table, c)
		if err != nil {
			if errors.As(err, errEmptyResults) {
				continue
			}
			return nil, fmt.Errorf("abalyzer.queryTooLongTextColumns: %w", err)
		}
		// The length of a mediumtext column
		if maxLen < 16777215 {
			res = append(res, newTooLongTextColumn(c, maxLen))
		}
	}
	return res, nil
}

func queryMaxLen(db *sql.DB, table string, col Column) (int, error) {
	rows, err := db.Query(fmt.Sprintf("select max(length(%s)) from %s", col.name, table))
	if err != nil {
		return -1, fmt.Errorf("analyzer.queryIndexes: exeuting query: %w", err)
	}
	defer rows.Close()

	var length int
	if rows.Next() {
		if err := rows.Scan(&length); err != nil {
			return -1, fmt.Errorf("analyzer.queryIndexes: scanning length: %w", err)
		}
	} else {
		return -1, fmt.Errorf("analyzer.queryMaxLan: %w", errEmptyResults)
	}
	return length, nil
}
