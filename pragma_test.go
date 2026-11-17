package sqlite_test

import (
	"context"
	"testing"

	"ella.to/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pragmaInt(t *testing.T, db *sqlite.Database, name string) int64 {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Done()
	stmt, err := conn.PrepareTransient(context.Background(), "PRAGMA "+name+";")
	require.NoError(t, err)
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	require.NoError(t, err)
	require.True(t, hasRow)
	return stmt.ColumnInt64(0)
}

func TestSynchronousDefaultsToNormal(t *testing.T) {
	assert.Equal(t, int64(1), pragmaInt(t, newFileDB(t), "synchronous"))
	assert.Equal(t, int64(2), pragmaInt(t, newFileDB(t, sqlite.WithSynchronous(sqlite.SynchronousFull)), "synchronous"))

	_, err := sqlite.New(context.Background(), sqlite.WithMemory(), sqlite.WithSynchronous("fast"))
	assert.Error(t, err)
}
