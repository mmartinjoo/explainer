package grade

const (
	MinGrade float32 = 1
	MaxGrade float32 = 5
)

type Grader interface {
	Grade() float32
}

func Dec(g, offset float32) float32 {
	return max(MinGrade, g-offset)
}
