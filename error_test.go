package eventsource_test

import (
	"io"
	"testing"

	"fmt"

	"github.com/altairsix/eventsource"
	"github.com/stretchr/testify/assert"
)

func TestNewError(t *testing.T) {
	err := eventsource.NewError(io.EOF, "code", "hello %v", "world")
	assert.NotNil(t, err)

	v, ok := err.(eventsource.Error)
	assert.True(t, ok)
	assert.Equal(t, io.EOF, v.Cause())
	assert.Equal(t, "code", v.Code())
	assert.Equal(t, "hello world", v.Message())
	assert.Equal(t, "[code] hello world - EOF", v.Error())

	s, ok := err.(fmt.Stringer)
	assert.True(t, ok)
	assert.Equal(t, v.Error(), s.String())
}
