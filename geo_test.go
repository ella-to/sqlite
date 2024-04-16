package sqlite_test

import (
	"testing"

	"ella.to/sqlite"
)

func TestCreateSqlCond(t *testing.T) {
	const expected = "(latitude > 1.396000 AND latitude < 1.396000 AND longitude < -0.698100 AND longitude > -0.698100)"

	cond := sqlite.CreateCondSQL(1.396, -0.6981, 0)
	if cond != expected {
		t.Errorf("expect '%s' but got this '%s'", expected, cond)
	}
}
