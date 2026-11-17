package sqlite

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

func LoadTime(stmt *Stmt, key string) time.Time {
	value := stmt.GetInt64(key)
	return time.Unix(value, 0).UTC()
}

// LoadBool follows SQLite's own truthiness: any non-zero integer is true.
func LoadBool(stmt *Stmt, key string) bool {
	return stmt.GetInt64(key) != 0
}

// jsonBufPool recycles the buffers JSON columns are copied into before
// decoding. json.Unmarshal copies everything it keeps (strings, RawMessage,
// []byte), so the buffer can be reused as soon as it returns.
var jsonBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}

const maxJSONBufRetain = 64 << 10

// loadJSON decodes the JSON in column col into v. It reports false, without
// touching v, when the column is NULL or empty.
func loadJSON(stmt *Stmt, col string, v any) (bool, error) {
	n := stmt.GetLen(col)
	if n == 0 {
		return false, nil
	}

	bp := jsonBufPool.Get().(*[]byte)
	if cap(*bp) < n {
		*bp = make([]byte, n)
	}
	b := (*bp)[:n]
	stmt.GetBytes(col, b)

	err := json.Unmarshal(b, v)

	if cap(*bp) <= maxJSONBufRetain {
		jsonBufPool.Put(bp)
	}
	return true, err
}

// LoadJsonMap decodes a JSON object column. A NULL or empty column (or the
// JSON null) yields an empty, non-nil map.
func LoadJsonMap[T any](stmt *Stmt, col string) (map[string]T, error) {
	var mapper map[string]T
	if _, err := loadJSON(stmt, col, &mapper); err != nil {
		return nil, err
	}
	if mapper == nil {
		return map[string]T{}, nil
	}
	return mapper, nil
}

// LoadJsonArray decodes a JSON array column. A NULL or empty column (or the
// JSON null) yields an empty, non-nil slice.
func LoadJsonArray[T any](stmt *Stmt, col string) ([]T, error) {
	var array []T
	if _, err := loadJSON(stmt, col, &array); err != nil {
		return nil, err
	}
	if array == nil {
		return []T{}, nil
	}
	return array, nil
}

// LoadJsonStruct decodes a JSON object column into a new T. A NULL or empty
// column yields the zero T.
func LoadJsonStruct[T any](stmt *Stmt, col string) (*T, error) {
	var obj T
	if _, err := loadJSON(stmt, col, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

// Placeholders returns a string of ? separated by commas
func Placeholders(count int) string {
	var sb strings.Builder
	placeholders(count, &sb)
	return sb.String()
}

func placeholders(count int, sb *strings.Builder) {
	for i := 0; i < count; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("?")
	}
}

func GroupPlaceholdersStringBuilder(numRows, numCols int, sb *strings.Builder) {
	for i := 0; i < numRows; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		placeholders(numCols, sb)
		sb.WriteString(")")
	}
}

// (?, ?), (?, ?), (?, ?)
func GroupPlaceholders(numRows, numCols int) string {
	var sb strings.Builder
	GroupPlaceholdersStringBuilder(numRows, numCols, &sb)
	return sb.String()
}

func ShowSql(sql string, args ...any) string {
	var temp2 []string

	temp := strings.FieldsFunc(sql, func(r rune) bool {
		switch r {
		case '\t', '\n', ' ':
			return true
		default:
			return false
		}
	})
	for _, tmp := range temp {
		if tmp != "" {
			temp2 = append(temp2, tmp)
		}
	}

	newArgs := []any{}
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			arg = fmt.Sprintf("'%s'", v)
		case time.Time:
			arg = fmt.Sprintf("%d", v.Unix())
		default:
			if v == nil {
				arg = "NULL"
				break
			}
			kind := reflect.TypeOf(v).Kind()
			if kind == reflect.Slice || kind == reflect.Map {
				b, _ := json.Marshal(v)
				arg = fmt.Sprintf("'%v'", string(b))
				break
			}
			arg = fmt.Sprintf("%v", v)
		}
		newArgs = append(newArgs, arg)
	}

	// escape literal % (LIKE patterns, ...) before turning ? into verbs
	format := strings.ReplaceAll(strings.Join(temp2, " "), "%", "%%")
	format = strings.ReplaceAll(format, "?", "%v")
	return fmt.Sprintf(format, newArgs...)
}
