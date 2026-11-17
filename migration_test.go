package sqlite_test

import (
	"context"
	"testing"
	"testing/fstest"

	"ella.to/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationFailureIsRetried(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)

	fsys := fstest.MapFS{
		"m/001.sql": {Data: []byte("CREATE TABLE ok (v INTEGER); THIS IS NOT SQL;")},
	}

	require.Error(t, sqlite.Migration(ctx, db, fsys, "m"))

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), countRows(t, conn, "migrations_sqlite"), "failed migration must not be recorded")
	_, err = conn.Prepare(ctx, "SELECT * FROM ok;")
	assert.Error(t, err, "partial migration must be rolled back")
	conn.Done()

	// the second run must try again (and fail again) rather than skip it
	require.Error(t, sqlite.Migration(ctx, db, fsys, "m"))

	// once fixed, it applies exactly once
	fsys["m/001.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE ok (v INTEGER);")}
	require.NoError(t, sqlite.Migration(ctx, db, fsys, "m"))
	require.NoError(t, sqlite.Migration(ctx, db, fsys, "m"))

	conn, err = db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()
	assert.Equal(t, int64(1), countRows(t, conn, "migrations_sqlite"))
	assert.Equal(t, int64(0), countRows(t, conn, "ok"))
}

func TestMigrationNestedDir(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	fsys := fstest.MapFS{
		"db/migrations/002.sql": {Data: []byte("INSERT INTO a (v) VALUES (2);")},
		"db/migrations/001.sql": {Data: []byte("CREATE TABLE a (v INTEGER);")},
		"db/migrations/readme":  {Data: []byte("not sql")},
	}
	require.NoError(t, sqlite.Migration(ctx, db, fsys, "db/migrations"))
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()
	assert.Equal(t, int64(1), countRows(t, conn, "a"))
	assert.Equal(t, int64(2), countRows(t, conn, "migrations_sqlite"))
}
