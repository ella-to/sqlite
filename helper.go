package sqlite

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

func LoadTime(stmt *Stmt, key string) time.Time {
	value := stmt.GetInt64(key)
	return time.Unix(value, 0)
}

func LoadBool(stmt *Stmt, key string) bool {
	return stmt.GetInt64(key) == 1
}

func LoadJsonMap[T any](stmt *Stmt, col string) (map[string]T, error) {
	var mapper map[string]T
	err := json.NewDecoder(stmt.GetReader(col)).Decode(&mapper)
	// NOTE: we need to check for io.EOF because json.NewDecoder returns io.EOF when the input is empty
	// this is not an error, we can just return an empty slice
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return mapper, nil
}

func LoadJsonArray[T any](stmt *Stmt, col string) ([]T, error) {
	var array []T
	err := json.NewDecoder(stmt.GetReader(col)).Decode(&array)
	// NOTE: we need to check for io.EOF because json.NewDecoder returns io.EOF when the input is empty
	// this is not an error, we can just return an empty slice
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return array, nil
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

	format := strings.ReplaceAll(strings.Join(temp2, " "), "?", "%v")
	return fmt.Sprintf(format, newArgs...)
}
