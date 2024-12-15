package platform

import (
	"fmt"
	"github.com/fatih/color"
)

type Result interface {
	Grader
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
