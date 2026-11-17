package sqlite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var (
	ErrNotFound    = errors.New("database row not found")
	ErrPrepareSQL  = errors.New("database failed to prepare sql")
	ErrExecSQL     = errors.New("database failed to exec sql")
	ErrUnknownType = errors.New("database failed to prepare sql because of unknown type")
	// ErrIntegerOverflow is returned when an unsigned value does not fit in
	// SQLite's signed 64-bit INTEGER.
	ErrIntegerOverflow = errors.New("database value overflows a signed 64-bit integer")
)

// Reason behind this is that I don't want to import two packages that
// starts with sqlite into my project. I can use the type alias in my project
type (
	Stmt              = sqlite.Stmt
	FunctionImpl      = sqlite.FunctionImpl
	Context           = sqlite.Context
	Value             = sqlite.Value
	AggregateFunction = sqlite.AggregateFunction
)

type ConnPrepareFunc func(*Conn) error

var IntegerValue = sqlite.IntegerValue

func Sql(sql string, values ...any) string {
	return fmt.Sprintf(sql, values...)
}

const (
	defaultCacheSizeKiB     = 16 * 1024
	defaultMmapSize         = 256 << 20
	defaultJournalSizeLimit = 64 << 20
)

// Database struct which holds pool of connection
type Database struct {
	stringConn    string
	pool          *sqlitex.Pool
	size          int
	prepareConnFn ConnPrepareFunc
	fns           map[string]*FunctionImpl
	synchronous   string
	cacheSizeKiB  int
	mmapSize      int64
	closed        atomic.Bool
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

func (db *Database) Exec(ctx context.Context, fn func(ctx context.Context, conn *Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Done()

	return fn(ctx, conn)
}

func (db *Database) put(conn *Conn) {
	db.pool.Put(conn.conn)
}

// Close closes all the connections in the pool
// and returns error if any connection fails to close
// NOTE: make sure to call this function at the end of your application
// Calling Close more than once is a no-op.
func (db *Database) Close() error {
	if db.closed.Swap(true) {
		return nil
	}
	db.optimize()
	return db.pool.Close()
}

// optimize runs PRAGMA optimize on one idle connection, as SQLite recommends
// before closing a long-lived database. It is best effort: it gives up if no
// connection becomes free quickly.
func (db *Database) optimize() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := db.pool.Take(ctx)
	if err != nil {
		return
	}
	defer db.pool.Put(conn)

	if err := sqlitex.ExecuteTransient(conn, `PRAGMA optimize;`, nil); err != nil {
		slog.Debug("sqlite: PRAGMA optimize failed", "error", err)
	}
}

type OptionFunc func(context.Context, *Database) error

// memoryDBCounter gives every in-memory database its own name, so two
// databases created with WithMemory in one process do not share state.
var memoryDBCounter atomic.Int64

// WithMemory uses a private in-memory database. All connections of the pool
// share it; other Database values do not.
func WithMemory() OptionFunc {
	return func(ctx context.Context, db *Database) error {
		name := fmt.Sprintf("file:memdb%d?mode=memory&cache=shared", memoryDBCounter.Add(1))
		return WithStringConn(name)(ctx, db)
	}
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
			slog.WarnContext(ctx, "stringConn changed", "old", db.stringConn, "new", stringConn)
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

// Values accepted by WithSynchronous, see https://sqlite.org/pragma.html#pragma_synchronous
const (
	SynchronousOff    = "OFF"
	SynchronousNormal = "NORMAL"
	SynchronousFull   = "FULL"
	SynchronousExtra  = "EXTRA"
)

// WithSynchronous sets PRAGMA synchronous on every connection. The default
// is NORMAL: in WAL mode it cannot corrupt the database, but the most recent
// commits can be lost on power loss (not on an application crash). Use FULL
// when every acknowledged commit must survive power loss.
func WithSynchronous(mode string) OptionFunc {
	return func(ctx context.Context, db *Database) error {
		switch mode {
		case SynchronousOff, SynchronousNormal, SynchronousFull, SynchronousExtra:
			db.synchronous = mode
			return nil
		default:
			return fmt.Errorf("invalid synchronous mode %q", mode)
		}
	}
}

// WithCacheSize sets the page cache size of every connection, in KiB.
// The default is 16 MiB per connection.
func WithCacheSize(kib int) OptionFunc {
	return func(ctx context.Context, db *Database) error {
		if kib <= 0 {
			return fmt.Errorf("invalid cache size %d KiB", kib)
		}
		db.cacheSizeKiB = kib
		return nil
	}
}

// WithMmapSize sets PRAGMA mmap_size, the number of bytes of the database
// file read through memory mapping. The default is 256 MiB; 0 disables it.
func WithMmapSize(bytes int64) OptionFunc {
	return func(ctx context.Context, db *Database) error {
		if bytes < 0 {
			return fmt.Errorf("invalid mmap size %d", bytes)
		}
		db.mmapSize = bytes
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
	var prepareConnMu sync.Mutex

	db := &Database{
		synchronous:  SynchronousNormal,
		cacheSizeKiB: defaultCacheSizeKiB,
		mmapSize:     defaultMmapSize,
	}
	for _, opt := range opts {
		err := opt(ctx, db)
		if err != nil {
			return nil, err
		}
	}

	// NOTE: these run outside any transaction on purpose, journal_mode and
	// synchronous cannot be changed inside one
	pragmas := []string{
		`PRAGMA foreign_keys = ON;`,
		`PRAGMA journal_mode = WAL;`,
		// in WAL mode FULL fsyncs the WAL on every commit, NORMAL only at
		// checkpoints
		`PRAGMA synchronous = ` + db.synchronous + `;`,
		// a negative value is a size in KiB
		fmt.Sprintf(`PRAGMA cache_size = -%d;`, db.cacheSizeKiB),
		`PRAGMA temp_store = MEMORY;`,
		fmt.Sprintf(`PRAGMA mmap_size = %d;`, db.mmapSize),
		// truncate the WAL back to this size after checkpoints instead of
		// letting it keep its high-water mark forever
		fmt.Sprintf(`PRAGMA journal_size_limit = %d;`, defaultJournalSizeLimit),
	}

	pool, err := sqlitex.NewPool(
		db.stringConn,
		sqlitex.PoolOptions{
			Flags:    0,
			PoolSize: db.size,
			PrepareConn: func(conn *sqlite.Conn) error {
				prepareConnMu.Lock()
				defer prepareConnMu.Unlock()

				for _, pragma := range pragmas {
					err := sqlitex.ExecuteTransient(conn, pragma, nil)
					if err != nil {
						return err
					}
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
