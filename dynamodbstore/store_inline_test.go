package dynamodbstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStore_CheckIdempotent(t *testing.T) {
	s := &Store{}
	err := s.checkIdempotent(context.Background(), "abc")
	assert.Nil(t, err, "no records provided; guaranteed idempotent!")
}
