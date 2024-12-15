package tableanalyzer

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/mmartinjoo/explainer/internal/platform/grade"
	"log"
	"slices"
	"strings"

	"github.com/mmartinjoo/explainer/internal/platform"
)

var errEmptyResults = errors.New("empty results")

type (
	Result struct {
		compositeIndexWarnings    []string
		stringBasedIndexWarning   string
		tooLongTextColumnsWarning string
		grade                     float32
	}

	Index struct {
		keyName     string
		indexType   string
		seq         int64
		column      string
		cardinality int64
	}

	Column struct {
		name     string
		dataType string
		key      string
	}

	CompositeIndexes map[string][]Index
	CompositeIndex   []Index
)

func newResult() Result {
	return Result{
		grade: 5,
	}
}

func Analyze(db *sql.DB, table string) error {
	log.Printf("Analyzing %s...\n", table)

	res, err := analyze(db, table)
	if err != nil {
		return fmt.Errorf("tableanalyzer.Analyze: %w", err)
	}

	platform.PrintResults(res)
	return nil
}

func analyze(db *sql.DB, table string) (Result, error) {
	res := newResult()
	res, err := res.analyzeCompositeIndexes(db, table)
	if err != nil {
		return res, fmt.Errorf("parser.analyze: %w", err)
	}
	res, err = res.analyzeStringIndexes(db, table)
	if err != nil {
		return res, fmt.Errorf("parser.analyze: %w", err)
	}
	res, err = res.analyzeTooLongTextColumns(db, table)
	if err != nil {
		return res, fmt.Errorf("parser.analyze: %w", err)
	}
	return res, nil
}

func (r Result) analyzeTooLongTextColumns(db *sql.DB, table string) (Result, error) {
	cols, err := queryTooLongTextColumns(db, table)
	if err != nil {
		return r, fmt.Errorf("analyzer.analyzeTooLongTextColumns: %w", err)
	}

	if len(cols) != 0 {
		var msg strings.Builder
		msg.WriteString("The following columns are type of longtext but based on the data in the table they should be smaller columns:\n")
		for _, c := range cols {
			msg.WriteString(fmt.Sprintf("- Column: %s, max length in table: %d", c.col.name, c.maxLen))
		}
		r.tooLongTextColumnsWarning = msg.String()
		r.grade = grade.Dec(r.grade, 0.25)
	}
	return r, nil
}

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

func (r Result) analyzeStringIndexes(db *sql.DB, table string) (Result, error) {
	stringCols, err := queryStringColumns(db, table)
	if err != nil {
		return r, fmt.Errorf("abalyzer.analyzeStringIndexes: querying columns: %w", err)
	}

	indexes, err := queryIndexes(db, table)
	if err != nil {
		return r, fmt.Errorf("abalyzer.analyzeStringIndexes: querying indexes: %w", err)
	}

	colsInIndex := make([]string, 0)
	for _, col := range stringCols {
		for _, idx := range indexes {
			if idx.column == col.name && idx.indexType != "FULLTEXT" {
				colsInIndex = append(colsInIndex, col.name)
			}
		}
	}

	slices.Compact(colsInIndex)

	if len(colsInIndex) > 0 {
		var msg strings.Builder
		msg.WriteString("The following string-based columns (varchar, text, mediumtext, etc) are being part of non-FULLTEXT indexes. ")
		msg.WriteString("It is usually a better idea to use a FULLTEXT index for columns like these because they are optimized for string data. On top of that, MySQL can only index the first 4KB of a text column so in case of a longer column it is only a partial index.\n")
		for _, v := range colsInIndex {
			if v == "" {
				continue
			}
			msg.WriteString(fmt.Sprintf("- %s\n", v))
		}
		r.stringBasedIndexWarning = msg.String()
		r.grade = grade.Dec(r.grade, 0.5)
	}
	return r, nil
}

func (r Result) analyzeCompositeIndexes(db *sql.DB, table string) (Result, error) {
	indexes, err := queryIndexes(db, table)
	if err != nil {
		return r, fmt.Errorf("analyzer.analyzeCompositeIndexes: %w", err)
	}
	compIndexes, err := findCompositeIndexes(indexes)
	if err != nil {
		return r, fmt.Errorf("analyzer.analyzeCompositeIndexes: %w", err)
	}

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
			msg.WriteString(fmt.Sprintf("'%s' is suboptimal. Columns are not ordered based on their cardinality which can result in expensive queries\n", name))
			msg.WriteString(fmt.Sprintf("The optimal column order should be: %v\n", optimalColOrder))
			msg.WriteString(fmt.Sprintf("But the actual column order is: %v\n", actualColOrder))
			r.compositeIndexWarnings = append(r.compositeIndexWarnings, msg.String())
		}
	}
	if len(r.compositeIndexWarnings) != 0 {
		r.grade = grade.Dec(r.grade, 2)
	}
	return r, nil
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

func (r Result) Grade() float32 {
	return r.grade
}

func (r Result) String() string {
	var str strings.Builder
	hasProblems := false
	str.WriteString(fmt.Sprintf("grade: %0.2f/%0.2f\n", r.grade, grade.MaxGrade))

	if len(r.compositeIndexWarnings) != 0 {
		hasProblems = true
		str.WriteString("Composite index problems:\n")
		for _, v := range r.compositeIndexWarnings {
			str.WriteString(fmt.Sprintf("- %s", v))
		}
	}
	if len(r.stringBasedIndexWarning) != 0 {
		hasProblems = true
		str.WriteString("String-based index problems:\n")
		str.WriteString(r.stringBasedIndexWarning)
	}
	if len(r.tooLongTextColumnsWarning) != 0 {
		hasProblems = true
		str.WriteString("\nToo long text columns:\n")
		str.WriteString(r.tooLongTextColumnsWarning)
	}

	if !hasProblems {
		str.WriteString("No problems found")
	}

	return str.String()
}
