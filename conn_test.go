package sqlite_test

import (
	"context"
	"sync"
	"testing"

	"ella.to/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestConnPrepareFunc(t *testing.T) {
	var conn *sqlite.Conn

	db, err := sqlite.New(
		sqlite.WithMemory(),
		sqlite.WithPoolSize(10),
		sqlite.WithConnPrepareFunc(func(conn *sqlite.Conn) error {

			err := conn.ExecScript(`CREATE TABLE IF NOT EXISTS mytests (name TEXT);`)
			if err != nil {
				return err
			}

			return nil
		}))
	assert.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	conn, err = db.Conn(context.Background())
	defer conn.Done()

	assert.NoError(t, err)
}

func TestConcurrentCalls(t *testing.T) {
	db, err := sqlite.New(
		sqlite.WithMemory(),
		sqlite.WithPoolSize(10),
		sqlite.WithConnPrepareFunc(func(conn *sqlite.Conn) error {
			err := conn.ExecScript(`CREATE TABLE IF NOT EXISTS mytests (name TEXT);`)
			if err != nil {
				return err
			}

			return nil
		}))
	assert.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	concurrentWorkers := 2
	totalCalls := 1000

	var wg sync.WaitGroup

	wg.Add(concurrentWorkers)

	for range concurrentWorkers {
		go func() {
			defer wg.Done()
			for range totalCalls {
				conn, err := db.Conn(context.Background())
				assert.NoError(t, err)
				_ = conn
				conn.Done()
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentCallsInsert(t *testing.T) {
	db, err := sqlite.New(
		sqlite.WithMemory(),
		sqlite.WithPoolSize(10),
		sqlite.WithConnPrepareFunc(func(conn *sqlite.Conn) error {
			err := conn.ExecScript(`CREATE TABLE IF NOT EXISTS mytests (name TEXT);`)
			if err != nil {
				return err
			}

			return nil
		}))
	assert.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	concurrentWorkers := 20
	totalCalls := 1000

	var wg sync.WaitGroup

	wg.Add(concurrentWorkers)

	for range concurrentWorkers {
		go func() {
			defer wg.Done()
			for range totalCalls {
				ctx := context.Background()
				conn, err := db.Conn(context.Background())
				assert.NoError(t, err)

				stmt, err := conn.Prepare(ctx, `INSERT INTO mytests (name) VALUES (?);`, "test")
				assert.NoError(t, err)
				_, err = stmt.Step()
				stmt.Finalize()
				assert.NoError(t, err)
				conn.Done()
			}
		}()
	}

	wg.Wait()

	conn, err := db.Conn(context.Background())
	assert.NoError(t, err)
	defer conn.Done()

	stmt, err := conn.Prepare(context.Background(), `SELECT COUNT(*) as count FROM mytests;`)
	assert.NoError(t, err)

	hasRow, err := stmt.Step()
	assert.NoError(t, err)
	assert.True(t, hasRow)
	count := stmt.GetInt64("count")
	stmt.Finalize()
	assert.Equal(t, int64(concurrentWorkers*totalCalls), count)
}
