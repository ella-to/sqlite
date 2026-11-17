package sqlite_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"ella.to/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConcurrentReadModifyWrite(t *testing.T, db *sqlite.Database) {
	ctx := context.Background()
	require.NoError(t, sqlite.RunScript(ctx, db, "CREATE TABLE counter (n INTEGER); INSERT INTO counter (n) VALUES (0);"))

	const workers, perWorker = 8, 50
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				errs <- db.Write(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
					// read, then write: fails with SQLITE_BUSY_SNAPSHOT
					// under a deferred transaction with several writers
					stmt, err := conn.Prepare(ctx, "SELECT n FROM counter;")
					if err != nil {
						return err
					}
					if _, err := stmt.Step(); err != nil {
						return err
					}
					n := stmt.GetInt64("n")
					if err := stmt.Reset(); err != nil {
						return err
					}
					stmt, err = conn.Prepare(ctx, "UPDATE counter SET n = ?;", n+1)
					if err != nil {
						return err
					}
					_, err = stmt.Step()
					return err
				})
			}
		}()
	}

	// readers running at the same time must not fail either
	var rwg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := db.Read(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
					stmt, err := conn.Prepare(ctx, "SELECT n FROM counter;")
					if err != nil {
						return err
					}
					_, err = stmt.Step()
					if rerr := stmt.Reset(); err == nil {
						err = rerr
					}
					return err
				})
				if err != nil {
					errs <- err
				}
			}
		}()
	}

	wg.Wait()
	close(stop)
	rwg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	err := db.Read(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		assert.Equal(t, int64(workers*perWorker), countOne(t, conn, "SELECT n AS c FROM counter;"))
		return nil
	})
	require.NoError(t, err)
}

func countOne(t *testing.T, conn *sqlite.Conn, sql string) int64 {
	t.Helper()
	stmt, err := conn.Prepare(context.Background(), sql)
	require.NoError(t, err)
	_, err = stmt.Step()
	require.NoError(t, err)
	c := stmt.GetInt64("c")
	require.NoError(t, stmt.Reset())
	return c
}

func TestWriteSerializesWriters(t *testing.T) {
	testConcurrentReadModifyWrite(t, newFileDB(t))
}

func TestWriteRollsBackOnError(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	require.NoError(t, sqlite.RunScript(ctx, db, "CREATE TABLE t (v INTEGER);"))

	err := db.Write(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, "INSERT INTO t (v) VALUES (1);")
		if err != nil {
			return err
		}
		if _, err := stmt.Step(); err != nil {
			return err
		}
		return errors.New("boom")
	})
	assert.EqualError(t, err, "boom")

	assert.Panics(t, func() {
		_ = db.Write(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
			stmt, _ := conn.Prepare(ctx, "INSERT INTO t (v) VALUES (2);")
			_, _ = stmt.Step()
			panic("boom")
		})
	})

	err = db.Read(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		assert.Equal(t, int64(0), countOne(t, conn, "SELECT count(*) AS c FROM t;"))
		return nil
	})
	require.NoError(t, err)

	// the writer connection is usable again after the panic
	require.NoError(t, db.Write(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, "INSERT INTO t (v) VALUES (3);")
		if err != nil {
			return err
		}
		_, err = stmt.Step()
		return err
	}))
}

func TestWriteSerializesWritersInMemory(t *testing.T) {
	db, err := sqlite.New(context.Background(), sqlite.WithMemory(), sqlite.WithPoolSize(4))
	require.NoError(t, err)
	defer db.Close()
	testConcurrentReadModifyWrite(t, db)
}
