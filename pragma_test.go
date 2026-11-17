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

func TestDefaultPragmas(t *testing.T) {
	db := newFileDB(t)
	assert.Equal(t, int64(-16*1024), pragmaInt(t, db, "cache_size"))
	assert.Equal(t, int64(256<<20), pragmaInt(t, db, "mmap_size"))
	assert.Equal(t, int64(64<<20), pragmaInt(t, db, "journal_size_limit"))

	db = newFileDB(t, sqlite.WithCacheSize(1024), sqlite.WithMmapSize(0))
	assert.Equal(t, int64(-1024), pragmaInt(t, db, "cache_size"))
	assert.Equal(t, int64(0), pragmaInt(t, db, "mmap_size"))
}

func TestCloseRunsOptimize(t *testing.T) {
	db := newFileDB(t)
	require.NoError(t, sqlite.RunScript(context.Background(), db, "CREATE TABLE t (v INTEGER); CREATE INDEX t_v ON t (v);"))
	require.NoError(t, db.Close())
}
