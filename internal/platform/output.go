package platform

import (
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"github.com/mmartinjoo/explainer/internal/platform/grade"
	"html"
	"os"
)

type Result interface {
	grade.Grader
	fmt.Stringer
}

func PrintResults(res Result) {
	if res.Grade() < 3 {
		color.Red(res.String() + "\n")
	}
	if res.Grade() >= 3 && res.Grade() < 4 {
		color.Yellow(res.String() + "\n")
	}
	if res.Grade() >= 4 {
		color.Green(res.String() + "\n")
	}
}

func VarDump(vars ...interface{}) {
	w := os.Stdout
	for i, v := range vars {
		fmt.Fprintf(w, "» item %d type %T:\n", i, v)
		j, err := json.MarshalIndent(v, "", "    ")
		switch {
		case err != nil:
			fmt.Fprintf(w, "error: %v", err)
		case len(j) < 3: // {}, empty struct maybe or empty string, usually mean unexported struct fields
			w.Write([]byte(html.EscapeString(fmt.Sprintf("%+v", v))))
		default:
			w.Write(j)
		}
		w.Write([]byte("\n\n"))
	}
}
