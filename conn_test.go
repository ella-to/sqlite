package sqlite_test

import (
	"context"
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
			ctx := context.Background()

			err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS mytests (name TEXT);`)
			if err != nil {
				return err
			}

			return nil
		}))
	assert.NoError(t, err)
	t.Cleanup(func() {
		conn.Done()
		db.Close()
	})

	conn, err = db.Conn(context.Background())
	assert.NoError(t, err)
}
