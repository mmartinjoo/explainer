# myexplainer

A CLI tool for analyzing queries and DB tables. It is meant to be used in local environment not in production.

It runs the following checks:
- Query access type
- Rows vs filtered rows in the EXPLAIN output
- Using filesort
- Using temporary
- Inefficient `SELECT *` queries
- Inefficient `LIKE %` statements
- Inefficient `JOIN` order
- Subqueries in `SELECT` statements
- Inefficient text columns
- Inefficient string-based indices
- Inefficient composite index order

The program gives you detailed explanations and tips on how to improve your queries and tables.

## Install

Download the binary for your OS.

MacOS (ARM 64bit):
```
wget https://github.com/mmartinjoo/explainer/releases/download/v0.0.1/myexplainer-darwin-arm64
```

Check out the [available binaries](https://github.com/mmartinjoo/explainer/releases) for your specific platform.

Or build from source:
```
git clone https://github.com/mmartinjoo/explainer
cd explainer
make build
./bin/<filename>
```

## Usage

``myexplainer table {tablename}`` 

analyzes the table structure and gives you performance-related warnings, if any.

``myexplainer logs {path}`` 

reads a log file in which every line contains a SQL query and analyzes them using EXPLAIN and gives you detailed information and tips

Examples:

``myexplainer --database analytics table page_views`` 

will analyze the 'page_views' table in the 'analytics' database on 'localhost' (default) with user 'root' (default) and password 'root' (default)

``myexplainer --database analytics logs ./queries.log`` 

will read the 'queries.log' file, parse the queries that it contains and then run EXPLAIN queries in the 'analytics' database on 'localhost' (default) with user 'root' (default) and password 'root' (default)

Flags:

- --database string Database name
- --help Show help message
- --host string Host address (default "localhost")
- --pass string Password (default "root")
- --port int Host port (default 3306)
- --user string Username (default "root")
- --version Show version