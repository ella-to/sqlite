package sqlite_test

import (
	"context"
	"encoding/json"
	"testing"

	"ella.to/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func selectCol(t *testing.T, conn *sqlite.Conn, value any) *sqlite.Stmt {
	t.Helper()
	stmt, err := conn.PrepareTransient(context.Background(), "SELECT ? AS j;", value)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stmt.Finalize() })
	hasRow, err := stmt.Step()
	require.NoError(t, err)
	require.True(t, hasRow)
	return stmt
}

func TestLoadJson(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()

	type obj struct {
		A   int             `json:"a"`
		Raw json.RawMessage `json:"raw"`
	}

	m, err := sqlite.LoadJsonMap[int](selectCol(t, conn, map[string]int{"x": 1}), "j")
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"x": 1}, m)

	for _, empty := range []any{nil, "", "null"} {
		m, err := sqlite.LoadJsonMap[int](selectCol(t, conn, empty), "j")
		require.NoError(t, err)
		assert.NotNil(t, m)
		assert.Empty(t, m)

		a, err := sqlite.LoadJsonArray[int](selectCol(t, conn, empty), "j")
		require.NoError(t, err)
		assert.NotNil(t, a)
		assert.Empty(t, a)

		o, err := sqlite.LoadJsonStruct[obj](selectCol(t, conn, empty), "j")
		require.NoError(t, err)
		assert.Equal(t, obj{}, *o)
	}

	a, err := sqlite.LoadJsonArray[string](selectCol(t, conn, []string{"a", "b"}), "j")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, a)

	// RawMessage must not alias the pooled buffer
	o1, err := sqlite.LoadJsonStruct[obj](selectCol(t, conn, `{"a":1,"raw":{"k":"first"}}`), "j")
	require.NoError(t, err)
	_, err = sqlite.LoadJsonStruct[obj](selectCol(t, conn, `{"a":2,"raw":{"k":"XXXXXXXXXXXX"}}`), "j")
	require.NoError(t, err)
	assert.JSONEq(t, `{"k":"first"}`, string(o1.Raw))

	_, err = sqlite.LoadJsonStruct[obj](selectCol(t, conn, `{"a":`), "j")
	assert.Error(t, err)
}
