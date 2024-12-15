package analyzer

import (
	"database/sql"
	"fmt"

	"github.com/mmartinjoo/explainer/internal/platform"
)

func AnalyzeTable(db *sql.DB, table string) error {
	rows, err := db.Query(fmt.Sprintf("show index from %s", table))
	if err != nil {
		return fmt.Errorf("analyzer.AnalyzeTable: exeuting query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("analyzer.AnalyzeTable: reading columns: %w", err)
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))

	indexes := make([]Index, 0)
	for rows.Next() {
		for i := range cols {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("analyzer.AnalyzeTable: scanning rows: %w", err)
		}

		var idx Index
		key, err := platform.ConvertString(values[2])
		if err != nil {
			return fmt.Errorf("analyzer.AnalyzeTable: parsing key: %w", err)
		}
		idx.keyName = key

		col, err := platform.ConvertString(values[4])
		if err != nil {
			return fmt.Errorf("analyzer.AnalyzeTable: parsing col: %w", err)
		}
		idx.column = col

		seq, ok := values[3].(int64)
		if !ok {
			return fmt.Errorf("analyzer.AnalyzeTable: parsing sequence: %w", err)
		}
		idx.seq = seq

		card, ok := values[6].(int64)
		if !ok {
			return fmt.Errorf("analyzer.AnalyzeTable: parsing cardinality: %w", err)
		}
		idx.cardinality = card

		indexes = append(indexes, idx)
	}

	fmt.Printf("indexes: %#v\n", indexes)
	return nil
}

type Index struct {
	keyName     string
	seq         int64
	column      string
	cardinality int64
}
