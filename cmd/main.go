package main

import (
	"database/sql"
	"fmt"
	"github.com/mmartinjoo/explainer/internal/explainer"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mmartinjoo/explainer/internal/tableanalyzer"
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
		if err = explainer.Explain(db, os.Args[2]); err != nil {
			log.Fatal(err)
		}
	case "table":
		if len(os.Args) != 3 {
			fmt.Printf("Usage:\nexplainer logs <path> to analyze a log file of SQL queries\nexplainer table <table> to analyze a table\n")
			os.Exit(1)
		}
		if err = tableanalyzer.Analyze(db, os.Args[2]); err != nil {
			log.Fatal(err)
		}
	default:
		fmt.Printf("Invalid argument: %s\n", os.Args[1])
		os.Exit(1)
	}
}
