package parser

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/mmartinjoo/explainer/internal/platform"
	"os"
	"slices"
	"strings"
)

func Parse(filename string) ([]platform.Query, error) {
	logs, err := readQueries(filename)
	if err != nil {
		return nil, fmt.Errorf("parser.Parse: %w", err)
	}
	queries, err := filterWriteQueries(logs)
	if err != nil {
		return nil, fmt.Errorf("parser.Parse: %w", err)
	}
	selectQueries, err := findSelectQueries(queries)
	if err != nil {
		return nil, fmt.Errorf("parser.Parse: %w", err)
	}
	queriesSub, err := substituteBindings(selectQueries)
	if err != nil {
		return nil, fmt.Errorf("parser.Parse: %w", err)
	}
	return queriesSub, nil
}

func readQueries(filename string) ([]string, error) {
	f, err := os.Open(filename)
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

func findSelectQueries(logLines []string) ([]string, error) {
	queries := make([]string, 0)
	for _, line := range logLines {
		idx := strings.Index(line, "select")
		if idx == -1 {
			continue
		}
		q := strings.Trim(line[idx:], " ")
		queries = append(queries, q)
	}
	return queries, nil
}

func substituteBindings(selectQueries []string) ([]platform.Query, error) {
	queries := make([]platform.Query, 0)
	for _, q := range selectQueries {
		if !hasBindings(q) {
			queries = append(queries, platform.NewQuery(q))
			continue
		}
		bindings, err := getBindings(q)
		if err != nil {
			return nil, fmt.Errorf("substituteBindings: %w", err)
		}
		c := strings.Count(q, "?")
		if c != len(bindings) {
			return nil, fmt.Errorf("argument number mismatch: %d \"?\" and the following bindings: %v", c, bindings)
		}
		idx := strings.LastIndex(q, "[")
		sql := strings.Trim(q[:idx], " ")
		queries = append(queries, platform.NewQueryWithBindings(sql, bindings))
	}
	return queries, nil
}

func hasBindings(query string) bool {
	return strings.Index(query, "]") == len(query)-1
}

func getBindings(query string) ([]string, error) {
	idx := strings.LastIndex(query, "[")
	if idx == -1 {
		return nil, fmt.Errorf("trying to parse bindings but \"[\" not found in query: %s", query)
	}

	// [10,20,30]
	bindingsStr := query[idx:]

	// Only one binding: [10]
	if !strings.Contains(bindingsStr, ",") {
		b := bindingsStr[1 : len(bindingsStr)-1]
		return []string{b}, nil
	}

	bindings := make([]string, 0)
	buf := bytes.Buffer{}
	for _, c := range bindingsStr {
		if c == '[' {
			continue
		}
		if c == ',' || c == ']' {
			bindings = append(bindings, buf.String())
			buf.Reset()
			continue
		}
		buf.WriteRune(c)
	}
	return bindings, nil
}
