package platform

const (
	MinGrade float32 = 1
	MaxGrade float32 = 5
)

type Grader interface {
	Grade() float32
}
