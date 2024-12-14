package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mmartinjoo/explainer/internal/analyzer"
	"github.com/mmartinjoo/explainer/internal/parser"
	"github.com/mmartinjoo/explainer/internal/runner"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3306)/analytics")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	queries, err := parser.Parse("/Users/joomartin/code/explainer/queries.log")
	if err != nil {
		panic(err)
	}

	explains, err := runner.Run(db, queries)
	if err != nil {
		panic(err)
	}

	res, err := analyzer.Analyze(explains)
	if err != nil {
		panic(err)
	}
	fmt.Printf("res: %s\n", res)
}
