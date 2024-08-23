package sqlite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var (
	ErrNotFound    = errors.New("database row not found")
	ErrPrepareSQL  = errors.New("database failed to prepare sql")
	ErrExecSQL     = errors.New("database failed to exec sql")
	ErrUnknownType = errors.New("database failed to prepare sql because of unknown type")
)

// Reason behind this is that I don't want to import two packages that
// starts with sqlite into my project. I can use the type alias in my project
type Stmt = sqlite.Stmt
type FunctionImpl = sqlite.FunctionImpl
type Context = sqlite.Context
type Value = sqlite.Value
type AggregateFunction = sqlite.AggregateFunction

type ConnPrepareFunc func(*Conn) error

var IntegerValue = sqlite.IntegerValue

func Sql(sql string, values ...any) string {
	return fmt.Sprintf(sql, values...)
}

// Database struct which holds pool of connection
type Database struct {
	pool          *sqlitex.Pool
	stringConn    string
	size          int
	remaining     int64
	prepareConnFn ConnPrepareFunc
	fns           map[string]*FunctionImpl
}

func (db *Database) PoolSize() int {
	return db.size
}

// Conn returns one connection from connection pool
// NOTE: make sure to call Done() to put the connection back to the pool
func (db *Database) Conn(ctx context.Context) (conn *Conn, err error) {
	sqlConn, err := db.pool.Take(ctx)
	if err != nil {
		return nil, err
	}

	conn = &Conn{
		conn:  sqlConn,
		put:   db.put,
		stmts: make(map[string]*Stmt),
	}

	poolSize := atomic.AddInt64(&db.remaining, -1)

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("get connection from pool", "pool_size", poolSize)
	}
	return conn, nil
}

func (db *Database) put(conn *sqlite.Conn) {
	db.pool.Put(conn)
	result := atomic.AddInt64(&db.remaining, 1)
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("put connection back to pool", "pool_size", result)
	}
}

func (db *Database) Close() error {
	remaining := atomic.LoadInt64(&db.remaining)
	if remaining != int64(db.size) {
		slog.Warn("database has some connections that haven't returned back to the pool", "still_open", int64(db.size)-remaining)
	}
	return db.pool.Close()
}

type OptionFunc func(*Database) error

func WithMemory() OptionFunc {
	return WithStringConn("file::memory:?mode=memory&cache=shared")
}

func WithFile(path string) OptionFunc {
	return func(db *Database) error {
		err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
		if err != nil {
			return err
		}

		return WithStringConn("file:" + path + "?cache=shared")(db)
	}
}

func WithStringConn(stringConn string) OptionFunc {
	return func(db *Database) error {
		if db.stringConn != "" {
			slog.Warn("stringConn changed", "old", db.stringConn, "new", stringConn)
		}
		db.stringConn = stringConn
		return nil
	}
}

func WithPoolSize(size int) OptionFunc {
	return func(db *Database) error {
		if db.size != 0 {
			return errors.New("pool size already set")
		}
		db.size = size
		db.remaining = int64(size)
		return nil
	}
}

func WithConnPrepareFunc(fn ConnPrepareFunc) OptionFunc {
	return func(db *Database) error {
		db.prepareConnFn = ConnPrepareFunc(fn)
		return nil
	}
}

func WithFunctions(fns map[string]*FunctionImpl) OptionFunc {
	return func(db *Database) error {
		db.fns = fns
		return nil
	}
}

// New creates a sqlite database
func New(opts ...OptionFunc) (*Database, error) {
	const pragma = `PRAGMA foreign_keys = ON;`

	var err error

	db := &Database{}
	for _, opt := range opts {
		err := opt(db)
		if err != nil {
			return nil, err
		}
	}

	db.pool, err = sqlitex.NewPool(db.stringConn, sqlitex.PoolOptions{
		PoolSize: db.size,
		PrepareConn: func(conn *sqlite.Conn) error {
			err = sqlitex.Execute(conn, pragma, nil)
			if err != nil {
				return err
			}

			for name, fn := range db.fns {
				err = conn.CreateFunction(name, fn)
				if err != nil {
					return err
				}
			}

			if db.prepareConnFn != nil {
				return db.prepareConnFn(&Conn{
					conn:  conn,
					put:   db.put,
					stmts: make(map[string]*Stmt),
				})
			}

			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	return db, nil
}

func RunScript(ctx context.Context, db *Database, sql string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Done()

	return conn.ExecScript(strings.TrimSpace(sql))
}

func RunScriptFiles(ctx context.Context, db *Database, path string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Done()

	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	sqlFiles := make([]string, 0, len(files))

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if filepath.Ext(file.Name()) != ".sql" {
			continue
		}

		sqlFile := filepath.Join(path, file.Name())
		sqlFiles = append(sqlFiles, sqlFile)
	}

	sort.Slice(sqlFiles, func(i, j int) bool {
		return sqlFiles[i] < sqlFiles[j]
	})

	for _, sqlFile := range sqlFiles {
		sql, err := os.ReadFile(sqlFile)
		if err != nil {
			return err
		}

		err = conn.ExecScript(string(sql))
		if err != nil {
			return err
		}
	}

	return nil
}
