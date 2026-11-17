package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"ella.to/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newFileDB(t *testing.T, opts ...sqlite.OptionFunc) *sqlite.Database {
	t.Helper()
	opts = append([]sqlite.OptionFunc{sqlite.WithFile(filepath.Join(t.TempDir(), "test.db")), sqlite.WithPoolSize(4)}, opts...)
	db, err := sqlite.New(context.Background(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func countRows(t *testing.T, conn *sqlite.Conn, table string) int64 {
	t.Helper()
	stmt, err := conn.Prepare(context.Background(), "SELECT count(*) AS c FROM "+table+";")
	require.NoError(t, err)
	hasRow, err := stmt.Step()
	require.NoError(t, err)
	require.True(t, hasRow)
	c := stmt.GetInt64("c")
	require.NoError(t, stmt.Reset())
	return c
}

func insertThenFail(ctx context.Context, conn *sqlite.Conn, begin func() (func(*error), error)) (err error) {
	end, err := begin()
	if err != nil {
		return err
	}
	defer end(&err)

	stmt, err := conn.Prepare(ctx, "INSERT INTO t (v) VALUES (?);", 1)
	if err != nil {
		return err
	}
	if _, err = stmt.Step(); err != nil {
		return err
	}
	return errors.New("boom")
}

func TestSaveRollsBackOnError(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()
	require.NoError(t, conn.ExecScript("CREATE TABLE t (v INTEGER);"))

	err = insertThenFail(ctx, conn, func() (func(*error), error) { return conn.Save(), nil })
	assert.EqualError(t, err, "boom")
	assert.Equal(t, int64(0), countRows(t, conn, "t"))

	err = insertThenFail(ctx, conn, conn.Immediate)
	assert.EqualError(t, err, "boom")
	assert.Equal(t, int64(0), countRows(t, conn, "t"))
}

func TestSaveCommitsOnSuccess(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()
	require.NoError(t, conn.ExecScript("CREATE TABLE t (v INTEGER);"))

	err = func() (err error) {
		defer conn.Save()(&err)
		stmt, err := conn.Prepare(ctx, "INSERT INTO t (v) VALUES (?);", 1)
		if err != nil {
			return err
		}
		_, err = stmt.Step()
		return err
	}()
	require.NoError(t, err)
	assert.Equal(t, int64(1), countRows(t, conn, "t"))
}

func TestMemoryDatabasesAreIsolated(t *testing.T) {
	ctx := context.Background()
	a, err := sqlite.New(ctx, sqlite.WithMemory(), sqlite.WithPoolSize(1))
	require.NoError(t, err)
	defer a.Close()
	b, err := sqlite.New(ctx, sqlite.WithMemory(), sqlite.WithPoolSize(1))
	require.NoError(t, err)
	defer b.Close()

	require.NoError(t, sqlite.RunScript(ctx, a, "CREATE TABLE only_in_a (v INTEGER);"))

	conn, err := b.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()
	_, err = conn.Prepare(ctx, "SELECT * FROM only_in_a;")
	assert.Error(t, err)
}

func TestLoadBoolNonZero(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()
	stmt, err := conn.Prepare(ctx, "SELECT 0 AS f, 1 AS t1, 2 AS t2, -1 AS t3;")
	require.NoError(t, err)
	_, err = stmt.Step()
	require.NoError(t, err)
	assert.False(t, sqlite.LoadBool(stmt, "f"))
	assert.True(t, sqlite.LoadBool(stmt, "t1"))
	assert.True(t, sqlite.LoadBool(stmt, "t2"))
	assert.True(t, sqlite.LoadBool(stmt, "t3"))
	require.NoError(t, stmt.Reset())
}

func TestConnDoneTwice(t *testing.T) {
	db := newFileDB(t, sqlite.WithPoolSize(1))
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	conn.Done()
	conn.Done()
	conn, err = db.Conn(context.Background())
	require.NoError(t, err)
	conn.Done()
}
