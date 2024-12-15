package analyzer

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/mmartinjoo/explainer/internal/platform"
)

func AnalyzeTable(db *sql.DB, table string) error {
	res := newTableAnalysisResult()

	indexes, err := findIndexes(db, table)
	if err != nil {
		return fmt.Errorf("analyzer.AnalyzeTable: %w", err)
	}
	compositeIndexes, err := findCompositeIndexes(indexes)
	if err != nil {
		return fmt.Errorf("analyzer.AnalyzeTable: %w", err)
	}
	res = res.analyzeCompositeIndexes(compositeIndexes)

	fmt.Printf("%#v\n", res)
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
type CompositeIndex []Index

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

func (r TableAnalysisResult) analyzeCompositeIndexes(compIndexes CompositeIndexes) TableAnalysisResult {
	for name, compIdx := range compIndexes {
		optimalIdx, ok := checkCardinality(compIdx)
		if !ok {
			var optimalColOrder []string
			for _, v := range optimalIdx {
				optimalColOrder = append(optimalColOrder, v.column)
			}

			var actualColOrder []string
			for _, v := range compIdx {
				actualColOrder = append(actualColOrder, v.column)
			}

			var msg strings.Builder
			msg.WriteString(fmt.Sprintf("The composite index '%s' is suboptimal. Columns are not ordered based on their cardinality which can result in expensive queries\n", name))
			msg.WriteString(fmt.Sprintf("The optimal column order should be: %v\n", optimalColOrder))
			msg.WriteString(fmt.Sprintf("But the actual column order is: %v\n", actualColOrder))
			r.CompositeIndexWarnings = append(r.CompositeIndexWarnings, msg.String())
			r.Grade = max(minGrade, r.Grade-1)
		}
	}
	return r
}

// checkCardinality checks if columns in a composite index are ordered based on their cardinality
// If it's not ordered well, the function returns the optimal index in the right order
func checkCardinality(compIdx CompositeIndex) (optimalIndex CompositeIndex, ok bool) {
	optimalIdx := make([]Index, len(compIdx))
	copy(optimalIdx, compIdx)

	slices.SortFunc(optimalIdx, func(a, b Index) int {
		return int(a.cardinality) - int(b.cardinality)
	})

	for i, v := range optimalIdx {
		if compIdx[i] != v {
			return optimalIdx, false
		}
	}
	return nil, true
}

type Index struct {
	keyName     string
	seq         int64
	column      string
	cardinality int64
}

type TableAnalysisResult struct {
	CompositeIndexWarnings []string
	Grade                  int
}

func newTableAnalysisResult() TableAnalysisResult {
	return TableAnalysisResult{
		Grade: 5,
	}
}
