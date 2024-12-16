package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mmartinjoo/explainer/internal/explainer"
	"github.com/mmartinjoo/explainer/internal/tableanalyzer"
	"log"
	"os"
)

const (
	name    = "myexplainer"
	version = "v0.0.1"
)

var (
	host     *string
	database *string
	port     *int
	user     *string
	pass     *string
)

func main() {
	host = flag.String("host", "localhost", "Host address")
	database = flag.String("database", "", "Database name")
	port = flag.Int("port", 3306, "Host port")
	user = flag.String("user", "root", "Username")
	pass = flag.String("pass", "root", "Password")
	help := flag.Bool("help", false, "Show help message")
	ver := flag.Bool("version", false, "Show version")

	flag.Parse()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s %s\n", name, version)
		fmt.Fprintf(os.Stderr, "==================\n")
		fmt.Fprintf(os.Stderr, "A CLI tool for analyzing queries and DB tables. It is meant to be used in local environment not in production.\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "'myexplainer table <tablename>' analyzes the table structure and gives you performance-related warnings, if any\n")
		fmt.Fprintf(os.Stderr, "'myexplainer logs <path>' reads a log file in which every line contains a SQL query and analyzes them using EXPLAIN and gives you detailed information and tips\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "'myexplainer --database analytics table page_views' will analyze the 'page_views' table in the 'analytics' database on 'localhost' (default) with user 'root' (default) and password 'root' (default)\n\n")
		fmt.Fprintf(os.Stderr, "'myexplainer --database analytics logs ./queries.log' will read the 'queries.log' file, parse the queries that it contains and then run EXPLAIN queries in the 'analytics' database on 'localhost' (default) with user 'root' (default) and password 'root' (default)\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}

	if *help {
		flag.Usage()
		return
	}
	if *ver {
		fmt.Printf("%s %s\n", name, version)
		return
	}

	db, err := sql.Open("mysql", connectionString())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if len(os.Args) < 2 {
		flag.Usage()
		return
	}

	param := os.Args[len(os.Args)-1]
	cmd := os.Args[len(os.Args)-2]

	switch cmd {
	case "logs":
		if err = explainer.Explain(db, param); err != nil {
			log.Fatal(err)
		}
	case "table":
		if err = tableanalyzer.Analyze(db, param); err != nil {
			log.Fatal(err)
		}
	default:
		flag.Usage()
		return
	}
}

func connectionString() string {
	// "root:root@tcp(127.0.0.1:3306)/analytics"
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", *user, *pass, *host, *port, *database)
}
