package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mmartinjoo/explainer/internal/analyzer"
	"github.com/mmartinjoo/explainer/internal/parser"
	"github.com/mmartinjoo/explainer/internal/runner"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3306)/paddle")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if len(os.Args) == 1 {
		fmt.Printf("Usage:\nexplainer logs <path> to analyze a log file of SQL queries\nexplainer table <table> to analyze a table\n")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "logs":
		if len(os.Args) != 3 {
			fmt.Printf("Usage:\nexplainer logs <path> to analyze a log file of SQL queries\nexplainer table <table> to analyze a table\n")
			os.Exit(1)
		}
		analyzeLogs(db, os.Args[2])
	case "table":
		if len(os.Args) != 3 {
			fmt.Printf("Usage:\nexplainer logs <path> to analyze a log file of SQL queries\nexplainer table <table> to analyze a table\n")
			os.Exit(1)
		}
		analyzeTable(db, os.Args[2])
	default:
		fmt.Printf("Invalid argument: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func analyzeTable(db *sql.DB, table string) {
	log.Printf("Analyzing %s...\n", table)

	res, err := analyzer.AnalyzeTable(db, table)
	if err != nil {
		panic(err)
	}

	if res.Grade <= 2 {
		color.Red(res.String() + "\n")
	}
	if res.Grade == 3 {
		color.Yellow(res.String() + "\n")
	}
	if res.Grade >= 4 {
		color.Green(res.String() + "\n")
	}
}

func analyzeLogs(db *sql.DB, path string) {
	queries, err := parser.Parse(path)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Analyzing %d unique queries...\n", len(queries))

	var tooManyConnectionsErr error
	explains, err := runner.Run(db, queries)
	if err != nil && !errors.As(err, &runner.TooManyConnectionsError{}) {
		log.Fatal(err)
	}
	if errors.As(err, &runner.TooManyConnectionsError{}) {
		tooManyConnectionsErr = err
	}

	results, err := analyzer.Analyze(explains)
	if err != nil {
		log.Fatal(err)
	}

	for _, res := range results {
		if res.Grade <= 2 {
			color.Red(res.String() + "\n")
		}
		if res.Grade == 3 {
			color.Yellow(res.String() + "\n")
		}
		if res.Grade >= 4 {
			color.Green(res.String() + "\n")
		}
	}

	log.Printf("%d unique queries were analyzed", len(explains))

	if tooManyConnectionsErr != nil {
		log.Println(tooManyConnectionsErr.Error())
	}
}
