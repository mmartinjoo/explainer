package explainer

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/mmartinjoo/explainer/internal/platform"
	"log"
	"slices"
	"strings"
)

func Explain(db *sql.DB, logFilePath string) error {
	queries, err := parseLogs(logFilePath)
	if err != nil {
		return fmt.Errorf("explainer.Explain: %w", err)
	}

	log.Printf("Analyzing %d unique queries...\n", len(queries))

	var tooManyConnectionsErr error
	explains, err := runExplainQueries(db, queries)
	if err != nil && !errors.As(err, &TooManyConnectionsError{}) {
		return fmt.Errorf("explainer.Explain: %w", err)
	}
	if errors.As(err, &TooManyConnectionsError{}) {
		tooManyConnectionsErr = err
	}

	results, err := analyze(db, explains)
	if err != nil {
		return fmt.Errorf("explainer.Explain: %w", err)
	}

	for _, res := range results {
		platform.PrintResults(res)
	}

	log.Printf("%d unique queries were analyzed", len(explains))

	if tooManyConnectionsErr != nil {
		return tooManyConnectionsErr
	}
	return nil
}

func analyze(db *sql.DB, explains []ExplainResult) ([]Result, error) {
	var results []Result
	for _, e := range explains {
		res := newResult(e)
		res = res.analyzeAccessType()
		res = res.analyzeFilteredRows()
		res = res.analyzeFilesort()
		res = res.analyzeTempTable()
		res = res.analyzeLikePattern()
		res = res.analyzeSelectStar()
		res = res.analyzeSubqueryInSelect()
		res, err := res.analyzeJoinOrder(db)
		if err != nil {
			log.Printf("unable to analyze join order: %s. Query: \"%s\"", err, e.Query.SQL)
		}

		results = append(results, res)
	}

	slices.SortFunc(results, func(a, b Result) int {
		if a.grade < b.grade {
			return 1
		}
		if a.grade > b.grade {
			return -1
		}
		return 0
	})
	return results, nil
}

type Result struct {
	explain                 ExplainResult
	accessTypeWarning       string
	filterWarning           string
	filesortWarning         string
	tempTableWarning        string
	selectStarWarning       string
	likePatternWarning      string
	joinOrderWarning        string
	subqueryInSelectWarning string
	grade                   float32
}

func newResult(expl ExplainResult) Result {
	return Result{
		explain: expl,
		grade:   5,
	}
}

func (r Result) Grade() float32 {
	return r.grade
}

func (r Result) String() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("Query: %s\n", r.explain.Query.SQL))
	str.WriteString(fmt.Sprintf("grade: %0.2f/%d\n", r.grade, platform.MaxGrade))

	if len(r.accessTypeWarning) != 0 {
		str.WriteString(fmt.Sprintf("Access type: %s\n", r.accessTypeWarning))
	}
	if len(r.filterWarning) != 0 {
		str.WriteString(fmt.Sprintf("Filtered rows: %s\n", r.filterWarning))
	}
	if len(r.filesortWarning) != 0 {
		str.WriteString(fmt.Sprintf("Filesort: %s\n", r.filesortWarning))
	}
	if len(r.tempTableWarning) != 0 {
		str.WriteString(fmt.Sprintf("Temp table: %s\n", r.tempTableWarning))
	}
	if len(r.likePatternWarning) != 0 {
		str.WriteString(fmt.Sprintf("Like pattern: %s\n", r.likePatternWarning))
	}
	if len(r.joinOrderWarning) != 0 {
		str.WriteString(fmt.Sprintf("Suboptimal join order: %s\n", r.joinOrderWarning))
	}
	if len(r.subqueryInSelectWarning) != 0 {
		str.WriteString(fmt.Sprintf("Subquery in SELECT: %s\n", r.subqueryInSelectWarning))
	}
	if len(r.selectStarWarning) != 0 {
		str.WriteString(fmt.Sprintf("Select: %s\n", r.selectStarWarning))
	}
	return str.String()
}

func (r Result) analyzeAccessType() Result {
	switch strings.ToLower(r.explain.QueryType.String) {
	case "all":
		r.accessTypeWarning = `The query uses the "ALL" access type. It scans ALL rows from the disk without using an index. It will cause you trouble if you have a large number of records.`
		r.grade = 1
	case "index":
		if !r.explain.UsingIndex() {
			r.accessTypeWarning = `Altough your query uses the "index" access type, the "Extra" column does not contain "Using index". It means you effectively do a FULL TABLE SCAN. First, the DB scans the whole BTREE index and then runs I/O operations for each node to satisfy the SELECT statement. It often happens when "SELECT *" is used. It will cause you trouble if you have a large number of records.`
			r.grade = 1
		} else {
			r.accessTypeWarning = `The query uses the "index" access type. It scans every node in the index BTREE which is pretty inefficient. It will cause you trouble if you have a large number of records. Fortunately, the "Extra" column contains "Using index" which means the query does not run a large number of extra I/O operations.`
			r.grade = 2
		}
	case "range":
		if !r.explain.UsingIndex() {
			r.accessTypeWarning = `Altough your query uses the "range" access type, the "Extra" column does not contain "Using index". It means you run unnecessary I/O operations. First, the DB scans the BTREE index for matching rows and then it runs I/O operations for each node. It can be slower if you have a large number of records.`
			r.grade = 3
		} else {
			r.accessTypeWarning = ""
			r.grade = 4
		}
	case "const":
	case "ref":
		r.accessTypeWarning = ""
		r.grade = 5
	}
	return r
}

func (r Result) analyzeFilteredRows() Result {
	if r.explain.Filtered.Float64 < 50.0 {
		r.grade = max(platform.MinGrade, r.grade-1)
		r.filterWarning = fmt.Sprintf("This query causes the DB to scan through %d rows but only returns %f%% of it. It usually happens when you have a composite index and the column order is not optimal. Or in the case of a full table scan.", r.explain.NumberOfRows.Int64, r.explain.Filtered.Float64)
	}
	return r
}

func (r Result) analyzeFilesort() Result {
	if r.explain.UsingFilesort() {
		r.grade = max(platform.MinGrade, r.grade-0.5)
		r.filesortWarning = "The query uses \"filesort\". It means that the DB cannot use the BTREE index to sort the results. It needs to copy the keys and then sort them separately. This can happen in-memory or on the disk. You probably sort or group based on a column that is not part of an index."
	}
	return r
}

func (r Result) analyzeTempTable() Result {
	if r.explain.UsingTemporary() {
		r.grade = max(platform.MinGrade, r.grade-0.5)
		r.tempTableWarning = "The query uses a \"temporary table\". The DB must create an in-memory or on-disk temporary table to hold intermediate results. It often happens when you use ORDER BY and GROUP BY together, especially when functions like COUNT() is used."
	}
	return r
}

func (r Result) analyzeSelectStar() Result {
	if r.explain.Query.HasSelectStar() {
		r.grade = max(platform.MinGrade, r.grade-0.25)
		r.selectStarWarning = "The query uses \"SELECT *\" which is usually not the best idea. It can increase the number of I/O operations, it uses more memory, makes TCP connections slower, and generally speaking slows down your query. If it's possible select only specific columns."
	}
	return r
}

func (r Result) analyzeLikePattern() Result {
	if r.explain.Query.HasLikePattern() {
		r.likePatternWarning = "The query has a \"LIKE %\" pattern in it which is usually not the most optimal solution. Consider using full-text index and full-text search."
		r.grade = max(platform.MinGrade, r.grade-0.5)
	}
	return r
}

func (r Result) analyzeJoinOrder(db *sql.DB) (Result, error) {
	tables := getJoinedTables(r.explain.Query.SQL)
	counts := make([]int, len(tables))

	for i, t := range tables {
		count, err := countRows(db, t)
		if err != nil {
			return r, fmt.Errorf("explainer.analyeJoinOrder: %w", err)
		}
		counts[i] = count
	}

	countsDesc := make([]int, len(counts))
	slices.SortFunc(countsDesc, func(a, b int) int {
		return b - a
	})

	if slices.Compare(counts, countsDesc) != 0 {
		r.joinOrderWarning = "Tables in the query might be joined in a suboptimal way. MySQL can perform better if you join smaller tables earlier and larger ones later. If it's possible, of course."
		r.grade = max(platform.MinGrade, r.grade-0.25)
	}

	return r, nil
}

func (r Result) analyzeSubqueryInSelect() Result {
	if r.explain.Query.HasSubqueryInSelect() {
		r.grade = max(platform.MinGrade, r.grade-2)
		r.subqueryInSelectWarning = "Usually, it's not a good idea to have a subquery in the SELECT clause. The database *might* run an additional query for every row in the result set. If your result contains 1,000 rows you might execute 1,000 additional SELECT queries. It's an N+1 query problem at the DB level."
	}
	return r
}

func getJoinedTables(sql string) []string {
	tables := make([]string, 0)
	sqlLower := strings.ToLower(sql)

	for {
		startIdx := strings.Index(sqlLower, "join ")
		if startIdx == -1 || startIdx+len("join ") >= len(sqlLower) {
			break
		}

		startIdx += len("join ")
		lenTableName := strings.Index(sqlLower[startIdx:], " ")
		if lenTableName == -1 {
			break
		}
		sqlLower = sqlLower[startIdx:]

		r := strings.NewReader(sqlLower)
		buf := make([]byte, lenTableName)
		r.Read(buf)
		tables = append(tables, string(buf))
	}
	return tables
}

func countRows(db *sql.DB, table string) (int, error) {
	rows, err := db.Query(fmt.Sprintf("select count(*) from %s", table))
	if err != nil {
		return -1, fmt.Errorf("explainer.CountRows: exeuting query: %w", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return -1, fmt.Errorf("explainer.CountRows: scanning count: %w", err)
		}
	}
	return count, nil
}
