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

	"ella.to/logger"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type Conn struct {
	conn *sqlite.Conn
	put  func(conn *Conn)
}

// When your try to use transaction in a nice way, you can use the following
// at the beginning of your code:
//
// defer conn.Save(&err)
func (c *Conn) Save(err *error) {
	sqlitex.Save(c.conn)(err)
}

// Done returns the connection back to the pool
func (c *Conn) Done() {
	c.put(c)
}

func (c *Conn) Prepare(ctx context.Context, sql string, values ...any) (*Stmt, error) {
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		logger.Debug(ctx, "prepare sql", "sql", ShowSql(sql, values...))
	}

	sql = strings.TrimSpace(sql)

	stmt, err := c.conn.Prepare(sql)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPrepareSQL, err)
	}

	var buffer bytes.Buffer

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
			buffer.Reset()
			// Encode the value to JSON
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
			stmt.BindInt64(i, v.UTC().Unix())
			continue
		case fmt.Stringer:
			stmt.BindText(i, v.String())
			continue
		}

		// Check for struct or pointer to struct
		// it will be encoded to JSON Map
		//
		// NOTE: this should be the last check as time.Time is also a struct
		// and we need to make sure we don't encode it to JSON
		if valueType.Kind() == reflect.Struct || (valueType.Kind() == reflect.Ptr && valueType.Elem().Kind() == reflect.Struct) {
			buffer.Reset()
			err = json.NewEncoder(&buffer).Encode(value)
			if err != nil {
				return nil, err
			}
			stmt.BindText(i, buffer.String())
			continue
		}

		return nil, ErrUnknownType
	}

	return stmt, nil
}

// Use this function to execute a script that contains multiple SQL statements
func (c *Conn) ExecScript(sql string) error {
	return sqlitex.ExecScript(c.conn, strings.TrimSpace(sql))
}
