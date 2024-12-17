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

Or build from source (needs go 1.23):
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

### Examples

**Analyzing a table**

``myexplainer --database analytics table page_views`` 

will analyze the 'page_views' table in the 'analytics' database on 'localhost' (default) with user 'root' (default) and password 'root' (default).

The output:
![output](https://i.ibb.co/7GTFCGF/myexplainer-logs.png)

**Analyzing a query log file**

``myexplainer --database analytics logs ./queries.log`` 

will read the 'queries.log' file, parse the queries that it contains and then run EXPLAIN queries in the 'analytics' database on 'localhost' (default) with user 'root' (default) and password 'root' (default).

The output:
![output](https://i.ibb.co/wyg2qM0/myexplainer-table.png)

A log file can look like this:
```
[2024-12-13 20:05:44] select * from `page_views` where `id` = ? [100]
select * from `page_views` where `id` IN (?,?,?) [100,200,300]
[2024-12-13 20:06:25] local.INFO: select * from `page_views`
```

The format of a line doesn't matter until it contains a `SELECT` query. If a log entry contains value bindings they must look like this:
```
select * from users where id = ? [1]
select * from users where id in (?,?) [1,2]
```

Placeholders must be `?` they are wrapped in `()` separated by `,` values are wrapped in `[]` separated by `,`

Flags:

- `--host` `string` Host address (default "localhost")
- `--port` `int` Host port (default 3306)
- `--user` `string` Username (default "root")
- `--pass` `string` Password (default "root")
- `--database` `string` Database name
- `--version` Show version
- `--help` Show help message

Using all flags:
```
myexplainer --host localhost --port 33060 --user admin --pass asdf1234 --database test 
```

*myexplainer is not battle tested yet. If you encounter a bug, please open an issue.*