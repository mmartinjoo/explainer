package analyzer

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/mmartinjoo/explainer/internal/platform"
)

var errEmptyResults = errors.New("empty results")

func AnalyzeTable(db *sql.DB, table string) (TableAnalysisResult, error) {
	res := newTableAnalysisResult()
	res, err := res.analyzeCompositeIndexes(db, table)
	if err != nil {
		return res, fmt.Errorf("parser.AnalyzeTable: %w", err)
	}
	res, err = res.analyzeStringIndexes(db, table)
	if err != nil {
		return res, fmt.Errorf("parser.AnalyzeTable: %w", err)
	}
	res, err = res.analyzeTooLongTextColumns(db, table)
	if err != nil {
		return res, fmt.Errorf("parser.AnalyzeTable: %w", err)
	}
	return res, nil
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

func (r TableAnalysisResult) analyzeTooLongTextColumns(db *sql.DB, table string) (TableAnalysisResult, error) {
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
		r.TooLongTextColumnsWarning = msg.String()
	}
	return r, nil
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

func (r TableAnalysisResult) analyzeStringIndexes(db *sql.DB, table string) (TableAnalysisResult, error) {
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

	if len(colsInIndex) > 0 {
		var msg strings.Builder
		msg.WriteString("The following string-based columns (varchar, text, mediumtext, etc) are being part of non-FULLTEXT indexes.")
		msg.WriteString("It is usually a better idea to use a FULLTEXT index for columns like these because they are optimized for string data. On top of that, MySQL can only index the first 4KB of a text column so in case of a longer column it is only a partial index.\n")
		for _, v := range colsInIndex {
			msg.WriteString(fmt.Sprintf("- %s\n", v))
		}
		r.StringBasedIndexWarning = msg.String()
	}
	return r, nil
}

func (r TableAnalysisResult) analyzeCompositeIndexes(db *sql.DB, table string) (TableAnalysisResult, error) {
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
			r.CompositeIndexWarnings = append(r.CompositeIndexWarnings, msg.String())
			r.Grade = max(minGrade, r.Grade-1)
		}
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

type Index struct {
	keyName     string
	indexType   string
	seq         int64
	column      string
	cardinality int64
}

type TableAnalysisResult struct {
	CompositeIndexWarnings           []string
	StringBasedIndexWarning          string
	TooLongTextColumnsWarning string
	Grade                            int
}

func newTableAnalysisResult() TableAnalysisResult {
	return TableAnalysisResult{
		Grade: 5,
	}
}

func (r TableAnalysisResult) String() string {
	var str strings.Builder
	hasProblems := false
	str.WriteString(fmt.Sprintf("Grade: %d/%d\n", r.Grade, maxGrade))

	if len(r.CompositeIndexWarnings) != 0 {
		hasProblems = true
		str.WriteString("Composite index problems:\n")
		for _, v := range r.CompositeIndexWarnings {
			str.WriteString(fmt.Sprintf("- %s", v))
		}
	}
	if len(r.StringBasedIndexWarning) != 0 {
		hasProblems = true
		str.WriteString("String-based index problems:\n")
		str.WriteString(r.StringBasedIndexWarning)
	}
	if len(r.TooLongTextColumnsWarning) != 0 {
		hasProblems = true
		str.WriteString("\nToo long text columns:\n")
		str.WriteString(r.TooLongTextColumnsWarning)
	}

	if !hasProblems {
		str.WriteString("No problems found")
	}

	return str.String()
}

type Column struct {
	name     string
	dataType string
	key      string
}
