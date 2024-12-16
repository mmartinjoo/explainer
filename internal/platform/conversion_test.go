package platform

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConvertString(t *testing.T) {
	val, err := ConvertString([]byte("str"))
	assert.Nil(t, err)
	assert.Equal(t, "str", val)
}
