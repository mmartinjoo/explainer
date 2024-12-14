package analyzer

import (
	"fmt"
	"strings"

	"github.com/mmartinjoo/explainer/internal/platform"
)

func Analyze(explains []platform.Explain) ([]Result, error) {
	var results []Result
	for _, e := range explains {
		res := newResult(e)
		res = res.analyzeAccessType()
		res = res.analyzeFilteredRows()
		res = res.analyzeFilesort()
		res = res.analyzeTempTable()
		results = append(results, res)
	}
	return results, nil
}

type Result struct {
	Explain           platform.Explain
	AccessTypeWarning string
	FilterWarning     string
	FilesortWarning   string
	TempTableWarning  string
	Grade             int
}

func newResult(expl platform.Explain) Result {
	return Result{
		Explain: expl,
		Grade:   5,
	}
}

func (r Result) String() string {
	return fmt.Sprintf("sql: %s, access type warn: %s, grade: %d", r.Explain.Query.SQL, r.AccessTypeWarning, r.Grade)
}

func (r Result) analyzeAccessType() Result {
	switch strings.ToLower(r.Explain.QueryType.String) {
	case "all":
		r.AccessTypeWarning = `The query uses the "ALL" access type. It scans ALL rows from the disk without using an index. It will cause you trouble if you have a large number of records.`
		r.Grade = 1
	case "index":
		if !r.Explain.UsingIndex() {
			r.AccessTypeWarning = `Altough your query uses the "index" access type, the "Extra" column does not contain "Using index". It means you effectively do a FULL TABLE SCAN. First, the DB scans the whole BTREE index and then runs I/O operations for each node. It will cause you trouble if you have a large number of records.`
			r.Grade = 1
		} else {
			r.AccessTypeWarning = `The query uses the "index" access type. It scans every node in the index BTREE which is pretty inefficient. It will cause you trouble if you have a large number of records. Fortunately, the "Extra" column contains "Using index" which means the query does not run a large number of extra I/O operations.`
			r.Grade = 2
		}
	case "range":
		if !r.Explain.UsingIndex() {
			r.AccessTypeWarning = `Altough your query uses the "range" access type, the "Extra" column does not contain "Using index". It means you run unnecessary I/O operations. First, the DB scans the BTREE index for matching rows and then it runs I/O operations for each node. It can be slower if you have a large number of records.`
			r.Grade = 3
		} else {
			r.AccessTypeWarning = ""
			r.Grade = 4
		}
	case "const":
	case "ref":
		r.AccessTypeWarning = ""
		r.Grade = 5
	}
	return r
}

func (r Result) analyzeFilteredRows() Result {
	if r.Explain.Filtered.Float64 < 50.0 {
		r.Grade = max(1, r.Grade-1)
		r.FilterWarning = fmt.Sprintf("This query causes the DB to scan through %d rows but only returns %f% of it. It usually happens when you have a composite index and the column order is not optimal.", r.Explain.NumberOfRows.Int64, r.Explain.Filtered.Float64)
	}
	return r
}

func (r Result) analyzeFilesort() Result {
	if r.Explain.UsingFilesort() {
		r.Grade = max(1, r.Grade-1)
		r.FilesortWarning = "The query uses \"filesort\". It means that the DB cannot use the BTREE index to sort the results. It needs to copy the keys and then sort them separately. This can happen in-memory or on the disk. You probably sort or group based on a column that is not part of an index."
	}
	return r
}

func (r Result) analyzeTempTable() Result {
	if r.Explain.UsingTemporary() {
		r.Grade = max(1, r.Grade-1)
		r.TempTableWarning = "The query uses a \"temporary table\". The DB must create an in-memory or on-disk temporary table to hold intermediate results. It often happens when you use ORDER BY and GROUP BY together, especially when functions like COUNT() is used."
	}
	return r
}
