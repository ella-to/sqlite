package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type Conn struct {
	conn *sqlite.Conn
	put  func(conn *Conn)
}

// Save opens a savepoint immediately and returns the function that releases
// it: it commits when *err is nil and rolls back otherwise. Use it at the
// beginning of a function with a named error result:
//
//	defer conn.Save()(&err)
//
// The savepoint must be opened before the statements run, which is why Save
// is called eagerly and only the returned function is deferred.
func (c *Conn) Save() func(*error) {
	return sqlitex.Save(c.conn)
}

// Immediate starts a BEGIN IMMEDIATE transaction, taking the database write
// lock up front. Use it for transactions that write, so a read-then-write
// transaction cannot fail with SQLITE_BUSY_SNAPSHOT halfway through:
//
//	end, err := conn.Immediate()
//	if err != nil {
//		return err
//	}
//	defer end(&err)
func (c *Conn) Immediate() (func(*error), error) {
	return sqlitex.ImmediateTransaction(c.conn)
}

// Done returns the connection back to the pool
func (c *Conn) Done() {
	if c == nil || c.put == nil {
		return
	}

	c.put(c)
}

// Prepare compiles sql (or reuses the copy this connection cached for the
// exact same string), binds values by position and returns the statement.
//
// Cached statements live as long as the connection, so pass a fixed set of
// SQL strings here. SQL assembled at runtime (Sql, Placeholders with a
// varying count, ...) grows that cache without bound: use PrepareTransient
// for it instead.
//
// A statement that has not been stepped until it returned false must be
// Reset before the connection is returned with Done, otherwise the pool
// panics on the still-active statement.
func (c *Conn) Prepare(ctx context.Context, sql string, values ...any) (*Stmt, error) {
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.DebugContext(ctx, "prepare sql", "sql", ShowSql(sql, values...))
	}

	stmt, err := c.conn.Prepare(strings.TrimSpace(sql))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPrepareSQL, err)
	}

	if err := bindValues(stmt, values); err != nil {
		return nil, err
	}

	return stmt, nil
}

// PrepareTransient is Prepare without the per-connection statement cache,
// meant for SQL built at runtime. The caller owns the statement and must
// call Finalize on it when done.
func (c *Conn) PrepareTransient(ctx context.Context, sql string, values ...any) (*Stmt, error) {
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.DebugContext(ctx, "prepare transient sql", "sql", ShowSql(sql, values...))
	}

	sql = strings.TrimSpace(sql)

	stmt, trailingBytes, err := c.conn.PrepareTransient(sql)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPrepareSQL, err)
	}
	if trailingBytes != 0 {
		_ = stmt.Finalize()
		return nil, fmt.Errorf("%w: statement has trailing bytes: %q", ErrPrepareSQL, sql)
	}

	if err := bindValues(stmt, values); err != nil {
		_ = stmt.Finalize()
		return nil, err
	}

	return stmt, nil
}

func bindValues(stmt *Stmt, values []any) error {
	for i, value := range values {
		// bind starts from 1
		if err := bindValue(stmt, i+1, value); err != nil {
			return err
		}
	}
	return nil
}

// Use this function to execute a script that contains multiple SQL statements
func (c *Conn) ExecScript(sql string) error {
	return sqlitex.ExecScript(c.conn, strings.TrimSpace(sql))
}

var timeType = reflect.TypeOf(time.Time{})

// bindValue binds one Go value to the 1-based parameter i of stmt.
func bindValue(stmt *Stmt, i int, value any) error {
	// fast path for the common types, no reflection
	switch v := value.(type) {
	case nil:
		stmt.BindNull(i)
		return nil
	case string:
		stmt.BindText(i, v)
		return nil
	case int:
		stmt.BindInt64(i, int64(v))
		return nil
	case int64:
		stmt.BindInt64(i, v)
		return nil
	case int32:
		stmt.BindInt64(i, int64(v))
		return nil
	case float64:
		stmt.BindFloat(i, v)
		return nil
	case bool:
		stmt.BindBool(i, v)
		return nil
	case []byte:
		stmt.BindBytes(i, v)
		return nil
	case json.RawMessage:
		stmt.BindBytes(i, v)
		return nil
	case time.Time:
		stmt.BindInt64(i, v.UTC().Unix())
		return nil
	}

	rv := reflect.ValueOf(value)
	valueType := rv.Type()

	if valueType.Kind() == reflect.Pointer {
		// a nil pointer is SQL NULL, never the JSON text "null"
		if rv.IsNil() {
			stmt.BindNull(i)
			return nil
		}
		elem := rv.Elem()
		if elem.Type() == timeType {
			// *time.Time binds like time.Time (Unix seconds), not via String()
			return bindValue(stmt, i, elem.Interface())
		}
		if v, ok := value.(fmt.Stringer); ok {
			stmt.BindText(i, v.String())
			return nil
		}
		if elem.Kind() == reflect.Struct && elem.Type() != timeType {
			return bindJSON(stmt, i, value)
		}
		// pointers to scalars (and *time.Time) bind as the value they point to
		return bindValue(stmt, i, elem.Interface())
	}

	switch valueType.Kind() {
	case reflect.Slice:
		if valueType.Elem().Kind() == reflect.Uint8 {
			// BindBytes binds an empty (non-NULL) blob for a zero length slice
			stmt.BindBytes(i, rv.Bytes())
			return nil
		}
		return bindJSON(stmt, i, value)
	case reflect.Map:
		return bindJSON(stmt, i, value)
	case reflect.String:
		stmt.BindText(i, rv.String())
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		stmt.BindInt64(i, rv.Int())
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := rv.Uint()
		if u > math.MaxInt64 {
			return fmt.Errorf("%w: parameter %d value %d", ErrIntegerOverflow, i, u)
		}
		stmt.BindInt64(i, int64(u))
		return nil
	case reflect.Float32, reflect.Float64:
		stmt.BindFloat(i, rv.Float())
		return nil
	case reflect.Bool:
		stmt.BindBool(i, rv.Bool())
		return nil
	}

	switch v := value.(type) {
	case time.Time:
		stmt.BindInt64(i, v.UTC().Unix())
		return nil
	case fmt.Stringer:
		stmt.BindText(i, v.String())
		return nil
	}

	// NOTE: this must stay after the time.Time check, time.Time is a struct
	// too and must not be encoded to JSON
	if valueType.Kind() == reflect.Struct {
		return bindJSON(stmt, i, value)
	}

	return ErrUnknownType
}

// bindJSON stores value as JSON text. The encoding (including the trailing
// newline json.Encoder writes) is part of the stored format; keep it.
func bindJSON(stmt *Stmt, i int, value any) error {
	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(value); err != nil {
		return err
	}
	stmt.BindText(i, buffer.String())
	return nil
}
