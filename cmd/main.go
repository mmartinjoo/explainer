package main

import (
	"fmt"
	"github.com/mmartinjoo/explainer/internal/parser"
)

func main() {
	queries, err := parser.Parse("/Users/joomartin/code/explainer/queries.log")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%#v\n", queries)
}
