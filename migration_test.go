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
