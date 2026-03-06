package scyllacdc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gocql/gocql"
)

// mockRequestError implements gocql.RequestError for testing.
type mockRequestError struct {
	code    int
	message string
}

func (e *mockRequestError) Error() string   { return e.message }
func (e *mockRequestError) Code() int       { return e.code }
func (e *mockRequestError) Message() string { return e.message }

// Verify mockRequestError implements gocql.RequestError.
var _ gocql.RequestError = (*mockRequestError)(nil)

func TestIsTableMissingError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "ErrNotFound",
			err:      gocql.ErrNotFound,
			expected: true,
		},
		{
			name:     "wrapped ErrNotFound",
			err:      fmt.Errorf("some context: %w", gocql.ErrNotFound),
			expected: true,
		},
		{
			name:     "ErrKeyspaceDoesNotExist",
			err:      gocql.ErrKeyspaceDoesNotExist,
			expected: true,
		},
		{
			name:     "wrapped ErrKeyspaceDoesNotExist",
			err:      fmt.Errorf("some context: %w", gocql.ErrKeyspaceDoesNotExist),
			expected: true,
		},
		{
			name:     "RequestError with no such table",
			err:      &mockRequestError{code: gocql.ErrCodeInvalid, message: "no such table ks.tbl"},
			expected: true,
		},
		{
			name:     "RequestError with unconfigured table",
			err:      &mockRequestError{code: gocql.ErrCodeInvalid, message: "unconfigured table tbl"},
			expected: true,
		},
		{
			name:     "RequestError with does not exist",
			err:      &mockRequestError{code: gocql.ErrCodeInvalid, message: "table ks.tbl does not exist"},
			expected: true,
		},
		{
			name:     "RequestError with wrong code but matching message falls through to string match",
			err:      &mockRequestError{code: gocql.ErrCodeSyntax, message: "no such table ks.tbl"},
			expected: true,
		},
		{
			name:     "RequestError with unrelated message",
			err:      &mockRequestError{code: gocql.ErrCodeInvalid, message: "invalid query syntax"},
			expected: false,
		},
		{
			name:     "generic error with no such table",
			err:      errors.New("no such table ks.tbl"),
			expected: true,
		},
		{
			name:     "generic error with unconfigured table",
			err:      errors.New("unconfigured table tbl"),
			expected: true,
		},
		{
			name:     "generic error with does not exist",
			err:      errors.New("keyspace does not exist"),
			expected: true,
		},
		{
			name:     "unrelated error",
			err:      errors.New("connection refused"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTableMissingError(tt.err)
			if got != tt.expected {
				t.Errorf("isTableMissingError(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}
