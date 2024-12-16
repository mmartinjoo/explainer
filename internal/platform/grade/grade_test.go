package grade

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDec(t *testing.T) {
	g := Dec(3, 1.25)
	assert.Equal(t, float32(1.75), g)
}

func TestDec_Overflow(t *testing.T) {
	g := Dec(1, 1.25)
	assert.Equal(t, float32(1), g)
}
