package sqlite_test

import (
	"testing"

	"ella.to/sqlite"
)

func TestPlaceholders(t *testing.T) {
	testCases := []struct {
		count int
		want  string
	}{
		{0, ""},
		{1, "?"},
		{2, "?, ?"},
		{3, "?, ?, ?"},
		{4, "?, ?, ?, ?"},
		{5, "?, ?, ?, ?, ?"},
	}

	for _, tc := range testCases {
		got := sqlite.Placeholders(tc.count)
		if got != tc.want {
			t.Errorf("Placeholders(%d) = %q; want %q", tc.count, got, tc.want)
		}
	}
}

func TestGroupPlaceholders(t *testing.T) {
	testCases := []struct {
		row  int
		col  int
		want string
	}{
		{0, 0, ""},
		{1, 1, "(?)"},
		{1, 2, "(?, ?)"},
		{2, 1, "(?), (?)"},
		{2, 2, "(?, ?), (?, ?)"},
		{2, 3, "(?, ?, ?), (?, ?, ?)"},
	}

	for _, tc := range testCases {
		got := sqlite.GroupPlaceholders(tc.row, tc.col)
		if got != tc.want {
			t.Errorf("GroupPlaceholders(%d, %d) = %q; want %q", tc.row, tc.col, got, tc.want)
		}
	}
}
