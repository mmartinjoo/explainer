// Package tableanalyzer is responsible for analyzing a table based on its columns and indexes
//
// Usage:
//
// db, _ := sql.Open("mysql", "<connectionString>")
//
//	if err := tableanalyzer.Analyze(db, "./queries.log"); err != nil {
//	    log.Fatal(err)
//	}
//
// This will print out the results to stdout
// The private [check] will return a [Result] object instead of printing the results
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

	res, err := check(db, table)
	if err != nil {
		return fmt.Errorf("tableanalyzer.Analyze: %w", err)
	}

	platform.PrintResults(&res)
	return nil
}

func check(db *sql.DB, table string) (Result, error) {
	res := newResult()
	if err := res.checkCompositeIndexes(db, table); err != nil {
		return res, fmt.Errorf("tableanalyzer.check: %w", err)
	}
	if err := res.checkStringIndexes(db, table); err != nil {
		return res, fmt.Errorf("tableanalyzer.check: %w", err)
	}
	if err := res.checkTooLongTextColumns(db, table); err != nil {
		return res, fmt.Errorf("tableanalyzer.check: %w", err)
	}
	return res, nil
}

// checkTooLongTextColumns checks if a column is too long compared the data it stores
//
// For example:
//   - If a column is mediumtext (can store up to 16m characters)
//   - But the longest string is 5000 characters
//   - It will mark this as a warning
func (r *Result) checkTooLongTextColumns(db *sql.DB, table string) error {
	cols, err := queryTooLongTextColumns(db, table)
	if err != nil {
		return fmt.Errorf("analyzer.checkTooLongTextColumns: %w", err)
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

// checkStringIndexes checks if varchar, text, mediumtext, etc columns are being used in indexes
func (r *Result) checkStringIndexes(db *sql.DB, table string) error {
	stringCols, err := queryStringColumns(db, table)
	if err != nil {
		return fmt.Errorf("abalyzer.checkStringIndexes: querying columns: %w", err)
	}

	indexes, err := queryIndexes(db, table)
	if err != nil {
		return fmt.Errorf("abalyzer.checkStringIndexes: querying indexes: %w", err)
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

// checkCompositeIndexes checks if columns are in the right order based on their cardinality
func (r *Result) checkCompositeIndexes(db *sql.DB, table string) error {
	indexes, err := queryIndexes(db, table)
	if err != nil {
		return fmt.Errorf("analyzer.checkCompositeIndexes: %w", err)
	}
	compIndexes, err := findCompositeIndexes(indexes)
	if err != nil {
		return fmt.Errorf("analyzer.checkCompositeIndexes: %w", err)
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
