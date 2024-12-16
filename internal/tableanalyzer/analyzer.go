package tableanalyzer

import (
	"database/sql"
	"fmt"
	"github.com/mmartinjoo/explainer/internal/platform/grade"
	"log"
	"slices"
	"strings"

	"github.com/mmartinjoo/explainer/internal/platform"
)

type (
	Result struct {
		compositeIndexWarnings    []string
		stringBasedIndexWarning   string
		tooLongTextColumnsWarning string
		grade                     float32
	}

	Column struct {
		name     string
		dataType string
		key      string
	}
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

	platform.PrintResults(&res)
	return nil
}

func analyze(db *sql.DB, table string) (Result, error) {
	res := newResult()
	if err := res.analyzeCompositeIndexes(db, table); err != nil {
		return res, fmt.Errorf("parser.analyze: %w", err)
	}
	if err := res.analyzeStringIndexes(db, table); err != nil {
		return res, fmt.Errorf("parser.analyze: %w", err)
	}
	if err := res.analyzeTooLongTextColumns(db, table); err != nil {
		return res, fmt.Errorf("parser.analyze: %w", err)
	}
	return res, nil
}

func (r *Result) analyzeTooLongTextColumns(db *sql.DB, table string) error {
	cols, err := queryTooLongTextColumns(db, table)
	if err != nil {
		return fmt.Errorf("analyzer.analyzeTooLongTextColumns: %w", err)
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
	return nil
}

func (r *Result) analyzeStringIndexes(db *sql.DB, table string) error {
	stringCols, err := queryStringColumns(db, table)
	if err != nil {
		return fmt.Errorf("abalyzer.analyzeStringIndexes: querying columns: %w", err)
	}

	indexes, err := queryIndexes(db, table)
	if err != nil {
		return fmt.Errorf("abalyzer.analyzeStringIndexes: querying indexes: %w", err)
	}

	colsInIndex := make([]string, 0)
	for _, col := range stringCols {
		for _, idx := range indexes {
			if idx.column == col.name && idx.indexType != "FULLTEXT" {
				colsInIndex = append(colsInIndex, col.name)
			}
		}
	}

	colsInIndex = slices.Compact(colsInIndex)
	if len(colsInIndex) > 0 {
		var msg strings.Builder
		msg.WriteString("The following string-based columns (varchar, text, mediumtext, etc) are being part of non-FULLTEXT indexes. ")
		msg.WriteString("It is usually a better idea to use a FULLTEXT index for columns like these because they are optimized for string data. On top of that, MySQL can only index the first 4KB of a text column so in case of a longer column it is only a partial index.\n")
		for _, v := range colsInIndex {
			msg.WriteString(fmt.Sprintf("- %s\n", v))
		}
		r.stringBasedIndexWarning = msg.String()
		r.grade = grade.Dec(r.grade, 0.5)
	}
	return nil
}

func (r *Result) analyzeCompositeIndexes(db *sql.DB, table string) error {
	indexes, err := queryIndexes(db, table)
	if err != nil {
		return fmt.Errorf("analyzer.analyzeCompositeIndexes: %w", err)
	}
	compIndexes, err := findCompositeIndexes(indexes)
	if err != nil {
		return fmt.Errorf("analyzer.analyzeCompositeIndexes: %w", err)
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
	return nil
}

func (r *Result) Grade() float32 {
	return r.grade
}

func (r *Result) String() string {
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
