package main

import (
	"database/sql"
	"fmt"
	"github.com/mmartinjoo/explainer/internal/explainer"
	"log"
	"os"

	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mmartinjoo/explainer/internal/analyzer"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3306)/analytics")
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
		err = explainer.Explain(db, os.Args[2])
		if err != nil {
			log.Fatal(err)
		}
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
