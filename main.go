package main

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strings"
)

func main() {
	logs, err := readQueries()
	if err != nil {
		panic(err)
	}
	queries, err := filterWriteQueries(logs)
	if err != nil {
		panic(err)
	}
	selectQueries, err := parseSelectQueries(queries)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%#v\n", selectQueries)
}

func readQueries() ([]string, error) {
	f, err := os.Open("queries.log")
	if err != nil {
		return nil, fmt.Errorf("readQueries: %w", err)
	}
	queries := make([]string, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		queries = append(queries, scanner.Text())
	}
	return queries, nil
}

func filterWriteQueries(logLines []string) ([]string, error) {
	writeCmds := []string{"insert", "update", "delete"}
	queries := make([]string, 0)
	for _, line := range logLines {
		if len(line) == 0 {
			continue
		}
		words := strings.Split(line, " ")
		isWriteCmd := false
		for _, w := range words {
			if slices.Contains(writeCmds, w) {
				isWriteCmd = true
				break
			}
		}
		if isWriteCmd {
			continue
		}
		queries = append(queries, line)
	}
	return queries, nil
}

func parseSelectQueries(logLines []string) ([]string, error) {
	queries := make([]string, 0)
	for _, line := range logLines {
		idx := strings.Index(line, "select")
		if idx == -1 {
			continue
		}
		queries = append(queries, line[idx:])
	}
	return queries, nil
}
