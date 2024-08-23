package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type Conn struct {
	conn  *sqlite.Conn
	stmts map[string]*Stmt
	put   func(conn *sqlite.Conn)
}

// When your try to use transaction in a nice way, you can use the following
// at the beginning of your code:
//
// defer conn.Save(&err)()
func (c *Conn) Save(err *error) func() {
	fn := sqlitex.Save(c.conn)
	return func() {
		fn(err)
	}
}

func (c *Conn) ExecScript(sql string) error {
	return sqlitex.ExecScript(c.conn, strings.TrimSpace(sql))
}

func (c *Conn) Done() error {
	for _, stmt := range c.stmts {
		if err := stmt.Finalize(); err != nil {
			return err
		}
	}

	c.put(c.conn)
	return nil
}

func (c *Conn) Warmup(sqls ...string) error {
	if len(c.stmts) > 0 {
		return fmt.Errorf("connection has already warmed up")
	}

	if len(sqls) == 0 {
		return nil
	}

	c.stmts = make(map[string]*Stmt, len(sqls))

	for _, sql := range sqls {
		stmt, err := c.conn.Prepare(sql)
		if err != nil {
			return err
		}
		c.stmts[sql] = stmt
	}

	return nil
}

func (c *Conn) Prepare(ctx context.Context, sql string, values ...any) (*Stmt, error) {
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("prepare sql", "sql", ShowSql(sql, values...))
	}

	sql = strings.TrimSpace(sql)

	stmt, err := c.conn.Prepare(sql)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPrepareSQL, err)
	}
	c.stmts[sql] = stmt

	for i, value := range values {
		i++ // bind starts from 1

		if value == nil {
			stmt.BindNull(i)
			continue
		}

		valueType := reflect.TypeOf(value)

		switch valueType.Kind() {
		case reflect.Slice:
			if valueType.Elem().Kind() == reflect.Uint8 {
				blob, ok := value.([]byte)
				if !ok {
					blob = value.(json.RawMessage)
				}
				stmt.BindZeroBlob(i, int64(len(blob)))
				stmt.BindBytes(i, blob)
				continue
			}
			fallthrough
		case reflect.Map:
			var buffer bytes.Buffer
			err = json.NewEncoder(&buffer).Encode(value)
			if err != nil {
				return nil, err
			}
			stmt.BindText(i, buffer.String())
			continue
		case reflect.String:
			stmt.BindText(i, reflect.ValueOf(value).String())
			continue
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			stmt.BindInt64(i, reflect.ValueOf(value).Int())
			continue
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			stmt.BindInt64(i, int64(reflect.ValueOf(value).Uint()))
			continue
		case reflect.Float32, reflect.Float64:
			stmt.BindFloat(i, reflect.ValueOf(value).Float())
			continue
		case reflect.Bool:
			stmt.BindBool(i, reflect.ValueOf(value).Bool())
			continue
		}

		switch v := value.(type) {
		case time.Time:
			stmt.BindInt64(i, v.Unix())
		case fmt.Stringer:
			stmt.BindText(i, v.String())
		default:
			return nil, ErrUnknownType
		}
	}

	return stmt, nil
}

func (c *Conn) Exec(ctx context.Context, sql string, values ...any) error {
	stmt, err := c.Prepare(ctx, sql, values...)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPrepareSQL, err)
	}

	_, err = stmt.Step()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrExecSQL, err)
	}
	return nil
}
