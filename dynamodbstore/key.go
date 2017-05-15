package dynamodbstore

import (
	"strconv"
	"strings"
)

const (
	// prefix prefixes the event keys in the dynamodb item
	prefix = "_"
)

func isKey(key string) bool {
	return strings.HasPrefix(key, prefix)
}

func makeKey(version int) string {
	return prefix + strconv.Itoa(version)
}

func versionFromKey(key string) (int, error) {
	if !strings.HasPrefix(key, prefix) {
		return 0, errInvalidKey
	}

	version, err := strconv.Atoi(key[len(prefix):])
	if err != nil {
		return 0, errInvalidKey
	}

	return version, nil
}
