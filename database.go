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
	stringConn    string
	prepareConnFn ConnPrepareFunc
	fns           map[string]*FunctionImpl
	size          int
	conns         chan *Conn
}

// Conn returns one connection from connection pool
// NOTE: make sure to call Done() to put the connection back to the pool
// usually right after this call, you should call defer conn.Done()
func (db *Database) Conn(ctx context.Context) (conn *Conn, err error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("get sqlite connection: %w", ctx.Err())
	case conn = <-db.conns:
		return conn, nil
	}
}

func (db *Database) put(conn *Conn) {
	db.conns <- conn
}

// Close closes all the connections in the pool
// and returns error if any connection fails to close
// NOTE: make sure to call this function at the end of your application
func (db *Database) Close() error {
	close(db.conns)

	for conn := range db.conns {
		err := conn.close()
		if err != nil {
			return err
		}
	}

	return nil
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
		db.size = size
		db.conns = make(chan *Conn, size)
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

	db := &Database{}
	for _, opt := range opts {
		err := opt(db)
		if err != nil {
			return nil, err
		}
	}

	for range db.size {
		conn, err := sqlite.OpenConn(db.stringConn)
		if err != nil {
			return nil, err
		}

		err = sqlitex.ExecScript(conn, pragma)
		if err != nil {
			return nil, err
		}

		for name, fn := range db.fns {
			err = conn.CreateFunction(name, fn)
			if err != nil {
				return nil, err
			}
		}

		c := &Conn{
			conn:  conn,
			stmts: make(map[string]*Stmt),
			put:   db.put,
		}

		if db.prepareConnFn != nil {
			err = db.prepareConnFn(c)
			if err != nil {
				return nil, err
			}
		}

		c.Done()
	}

	return db, nil
}

func RunScript(ctx context.Context, db *Database, sql string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Done()

	return conn.Exec(ctx, strings.TrimSpace(sql))
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

		err = conn.Exec(ctx, string(sql))
		if err != nil {
			return err
		}
	}

	return nil
}
