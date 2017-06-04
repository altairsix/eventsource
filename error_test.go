package eventsource_test

import (
	"io"
	"testing"

	"fmt"

	"github.com/altairsix/eventsource"
	"github.com/pkg/errors"
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

func TestIsNotFound(t *testing.T) {
	testCases := map[string]struct {
		Err        error
		IsNotFound bool
	}{
		"nil": {
			Err:        nil,
			IsNotFound: false,
		},
		"eventsource.Error": {
			Err:        eventsource.NewError(nil, eventsource.ErrAggregateNotFound, "not found"),
			IsNotFound: true,
		},
		"nested eventsource.Error": {
			Err: eventsource.NewError(
				eventsource.NewError(nil, eventsource.ErrAggregateNotFound, "not found"),
				eventsource.ErrUnboundEventType,
				"not found",
			),
			IsNotFound: true,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			assert.Equal(t, tc.IsNotFound, eventsource.IsNotFound(tc.Err))
		})
	}
}

func TestErrHasCode(t *testing.T) {
	code := "code"

	testCases := map[string]struct {
		Err        error
		ErrHasCode bool
	}{
		"simple": {
			Err:        eventsource.NewError(nil, code, "blah"),
			ErrHasCode: true,
		},
		"nope": {
			Err:        errors.New("blah"),
			ErrHasCode: false,
		},
		"nested": {
			Err:        eventsource.NewError(eventsource.NewError(nil, code, "blah"), "blah", "blah"),
			ErrHasCode: true,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			assert.Equal(t, tc.ErrHasCode, eventsource.ErrHasCode(tc.Err, code))
		})
	}
}
