package sqlite_test

import (
	"context"
	"sync"
	"testing"

	"ella.to/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestConnPrepareFunc(t *testing.T) {
	ctx := context.Background()

	var conn *sqlite.Conn

	db, err := sqlite.New(
		ctx,
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
	ctx := context.Background()

	db, err := sqlite.New(
		ctx,
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

func TestAddingStructToJson(t *testing.T) {
	ctx := context.Background()

	db, err := sqlite.New(
		ctx,
		sqlite.WithMemory(),
		sqlite.WithPoolSize(10),
		sqlite.WithConnPrepareFunc(func(conn *sqlite.Conn) error {
			err := conn.ExecScript(`CREATE TABLE IF NOT EXISTS mytests (map JSON);`)
			if err != nil {
				return err
			}

			return nil
		}))
	assert.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	type TestStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	conn, err := db.Conn(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Done() })

	stmt, err := conn.Prepare(ctx, `INSERT INTO mytests (map) VALUES (?);`, TestStruct{Name: "test", Age: 10})
	assert.NoError(t, err)
	t.Cleanup(func() { stmt.Finalize() })

	_, err = stmt.Step()
	assert.NoError(t, err)

	stmt, err = conn.Prepare(ctx, `SELECT map FROM mytests;`)
	assert.NoError(t, err)
	hasRow, err := stmt.Step()

	assert.NoError(t, err)
	assert.True(t, hasRow)

	result, err := sqlite.LoadJsonStruct[TestStruct](stmt, "map")
	assert.NoError(t, err)

	assert.Equal(t, "test", result.Name)
	assert.Equal(t, 10, result.Age)
}

func TestAddingPointerStructToJson(t *testing.T) {
	ctx := context.Background()

	db, err := sqlite.New(
		ctx,
		sqlite.WithMemory(),
		sqlite.WithPoolSize(10),
		sqlite.WithConnPrepareFunc(func(conn *sqlite.Conn) error {
			err := conn.ExecScript(`CREATE TABLE IF NOT EXISTS mytests (map JSON);`)
			if err != nil {
				return err
			}

			return nil
		}))
	assert.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	type TestStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	conn, err := db.Conn(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Done() })

	stmt, err := conn.Prepare(ctx, `INSERT INTO mytests (map) VALUES (?);`, &TestStruct{Name: "test", Age: 10})
	assert.NoError(t, err)
	t.Cleanup(func() { stmt.Finalize() })

	_, err = stmt.Step()
	assert.NoError(t, err)

	stmt, err = conn.Prepare(ctx, `SELECT map FROM mytests;`)
	assert.NoError(t, err)
	hasRow, err := stmt.Step()

	assert.NoError(t, err)
	assert.True(t, hasRow)

	result, err := sqlite.LoadJsonStruct[TestStruct](stmt, "map")
	assert.NoError(t, err)

	assert.Equal(t, "test", result.Name)
	assert.Equal(t, 10, result.Age)
}

func TestConcurrentCallsInsert(t *testing.T) {
	ctx := context.Background()

	db, err := sqlite.New(
		ctx,
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
