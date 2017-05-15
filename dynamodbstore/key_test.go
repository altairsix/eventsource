package dynamodbstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKey(t *testing.T) {
	version := 1
	key := makeKey(version)
	assert.True(t, isKey(key))

	found, err := versionFromKey(key)
	assert.Nil(t, err)
	assert.Equal(t, version, found)
}

func TestVersionFromKey(t *testing.T) {
	testCases := map[string]struct {
		Key      string
		Version  int
		HasError bool
	}{
		"simple": {
			Key:     "_1",
			Version: 1,
		},
		"invalid-prefix": {
			Key:      "1",
			HasError: true,
		},
		"invalid-version": {
			Key:      "_a",
			HasError: true,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			version, err := versionFromKey(tc.Key)
			if tc.HasError {
				assert.Equal(t, errInvalidKey, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tc.Version, version)
			}
		})
	}
}
