package analyzer

import (
	"database/sql"
	"fmt"
	"slices"

	"github.com/mmartinjoo/explainer/internal/platform"
)

func AnalyzeTable(db *sql.DB, table string) error {
	indexes, err := findIndexes(db, table)
	if err != nil {
		return fmt.Errorf("analyzer.AnalyzeTable: %w", err)
	}

	compositeIndexes, err := findCompositeIndexes(indexes)
	if err != nil {
		return fmt.Errorf("analyzer.AnalyzeTable: %w", err)
	}

	fmt.Printf("%#v\n", compositeIndexes)
	return nil
}

func findIndexes(db *sql.DB, table string) ([]Index, error) {
	rows, err := db.Query(fmt.Sprintf("show index from %s", table))
	if err != nil {
		return nil, fmt.Errorf("analyzer.findIndexes: exeuting query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("analyzer.findIndexes: reading columns: %w", err)
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))

	indexes := make([]Index, 0)
	for rows.Next() {
		for i := range cols {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("analyzer.findIndexes: scanning rows: %w", err)
		}

		var idx Index
		key, err := platform.ConvertString(values[2])
		if err != nil {
			return nil, fmt.Errorf("analyzer.findIndexes: parsing key: %w", err)
		}
		idx.keyName = key

		col, err := platform.ConvertString(values[4])
		if err != nil {
			return nil, fmt.Errorf("analyzer.findIndexes: parsing col: %w", err)
		}
		idx.column = col

		seq, ok := values[3].(int64)
		if !ok {
			return nil, fmt.Errorf("analyzer.findIndexes: parsing sequence: %w", err)
		}
		idx.seq = seq

		card, ok := values[6].(int64)
		if !ok {
			return nil, fmt.Errorf("analyzer.findIndexes: parsing cardinality: %w", err)
		}
		idx.cardinality = card

		indexes = append(indexes, idx)
	}
	return indexes, nil
}

type CompositeIndexes map[string][]Index

func findCompositeIndexes(indexes []Index) (CompositeIndexes, error) {
	hmap := make(CompositeIndexes)
	for _, idx := range indexes {
		hmap[idx.keyName] = append(hmap[idx.keyName], idx)
	}
	for k, v := range hmap {
		if len(v) == 1 {
			delete(hmap, k)
		}
	}
	for k := range hmap {
		slices.SortFunc(hmap[k], func(a, b Index) int {
			return int(a.seq) - int(b.seq)
		})
	}
	return hmap, nil
}

type Index struct {
	keyName     string
	seq         int64
	column      string
	cardinality int64
}
