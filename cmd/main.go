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

func main() {
	opts, err := newOptsFromOSArgs()
	if err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("mysql", opts.connectionString())
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

type Opts struct {
	host string
	port int
	user string
	pass string
	db   string
}

func newOptsFromOSArgs() (Opts, error) {
	host := flag.String("host", "localhost", "Host address")
	db := flag.String("database", "", "Database")
	port := flag.Int("port", 3306, "Host port")
	user := flag.String("user", "root", "Username")
	pass := flag.String("pass", "root", "Password")
	flag.Parse()

	if *host == "" || *db == "" || *port == 0 || *user == "" || *pass == "" {
		return Opts{}, ValidationError{}
	}

	return Opts{
		host: *host,
		db:   *db,
		port: *port,
		user: *user,
		pass: *pass,
	}, nil
}

func (o Opts) connectionString() string {
	// "root:root@tcp(127.0.0.1:3306)/analytics"
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", o.user, o.pass, o.host, o.port, o.db)
}

type ValidationError struct {
	field string
	value any
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("--host --port --database --user --pass cannot be empty")
}
