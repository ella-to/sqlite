package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"ella.to/logger"
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
	pool          *sqlitex.Pool
	size          int
	prepareConnFn ConnPrepareFunc
	fns           map[string]*FunctionImpl
}

// Conn returns one connection from connection pool
// NOTE: make sure to call Done() to put the connection back to the pool
// usually right after this call, you should call defer conn.Done()
func (db *Database) Conn(ctx context.Context) (*Conn, error) {
	conn, err := db.pool.Take(ctx)
	if err != nil {
		return nil, err
	}

	return &Conn{
		conn: conn,
		put:  db.put,
	}, nil
}

func (db *Database) Exec(ctx context.Context, fn func(ctx context.Context, conn *Conn)) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Done()

	fn(ctx, conn)

	return nil
}

func (db *Database) put(conn *Conn) {
	db.pool.Put(conn.conn)
}

// Close closes all the connections in the pool
// and returns error if any connection fails to close
// NOTE: make sure to call this function at the end of your application
func (db *Database) Close() error {
	return db.pool.Close()
}

type OptionFunc func(context.Context, *Database) error

func WithMemory() OptionFunc {
	return WithStringConn("file::memory:?mode=memory&cache=shared")
}

func WithFile(path string) OptionFunc {
	return func(ctx context.Context, db *Database) error {
		err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
		if err != nil {
			return err
		}

		return WithStringConn("file:"+path+"?cache=shared")(ctx, db)
	}
}

func WithStringConn(stringConn string) OptionFunc {
	return func(ctx context.Context, db *Database) error {
		if db.stringConn != "" {
			logger.Warn(ctx, "stringConn changed", "old", db.stringConn, "new", stringConn)
		}
		db.stringConn = stringConn
		return nil
	}
}

func WithPoolSize(size int) OptionFunc {
	return func(ctx context.Context, db *Database) error {
		db.size = size
		return nil
	}
}

func WithConnPrepareFunc(fn ConnPrepareFunc) OptionFunc {
	return func(ctx context.Context, db *Database) error {
		db.prepareConnFn = ConnPrepareFunc(fn)
		return nil
	}
}

func WithFunctions(fns map[string]*FunctionImpl) OptionFunc {
	return func(ctx context.Context, db *Database) error {
		db.fns = fns
		return nil
	}
}

// New creates a sqlite database
func New(ctx context.Context, opts ...OptionFunc) (*Database, error) {
	pragma := strings.TrimSpace(`
		PRAGMA foreign_keys = ON;
		PRAGMA journal_mode = WAL;
		PRAGMA cache_size = -2000;  -- Use negative value for KB size (here, 2MB)
		PRAGMA temp_store = MEMORY;
	`)

	db := &Database{}
	for _, opt := range opts {
		err := opt(ctx, db)
		if err != nil {
			return nil, err
		}
	}

	pool, err := sqlitex.NewPool(
		db.stringConn,
		sqlitex.PoolOptions{
			Flags:    0,
			PoolSize: db.size,
			PrepareConn: func(conn *sqlite.Conn) error {
				err := sqlitex.ExecScript(conn, pragma)
				if err != nil {
					return err
				}

				if db.prepareConnFn != nil {
					return db.prepareConnFn(&Conn{conn: conn, put: func(conn *Conn) {}})
				}

				return nil
			},
		},
	)
	if err != nil {
		return nil, err
	}

	db.pool = pool

	return db, nil
}

func RunScript(ctx context.Context, db *Database, sql string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Done()

	return sqlitex.ExecScript(conn.conn, sql)
}

func RunScriptFiles(ctx context.Context, db *Database, path string) error {
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

		err = RunScript(ctx, db, string(sql))
		if err != nil {
			return err
		}
	}

	return nil
}
