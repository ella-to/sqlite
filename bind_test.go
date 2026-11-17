package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"ella.to/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// typeOf returns the SQLite storage class and the text form of the bound value.
func bindRoundTrip(t *testing.T, conn *sqlite.Conn, value any) (string, string) {
	t.Helper()
	stmt, err := conn.Prepare(context.Background(), "SELECT typeof(?1) AS ty, CAST(?1 AS TEXT) AS v;", value)
	require.NoError(t, err)
	hasRow, err := stmt.Step()
	require.NoError(t, err)
	require.True(t, hasRow)
	ty, v := stmt.GetText("ty"), stmt.GetText("v")
	require.NoError(t, stmt.Reset())
	return ty, v
}

type namedString string
type namedInt int

type payload struct {
	A int `json:"a"`
}

func TestBindValues(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()

	s := "hi"
	n := int64(42)
	f := 1.5
	b := true
	u := uint32(7)
	ts := time.Unix(1700000000, 0)
	var nilStr *string
	var nilStruct *payload

	cases := []struct {
		name   string
		value  any
		wantTy string
		wantV  string
	}{
		{"nil", nil, "null", ""},
		{"string", "x", "text", "x"},
		{"named string", namedString("y"), "text", "y"},
		{"int", 3, "integer", "3"},
		{"named int", namedInt(4), "integer", "4"},
		{"uint64 max int", uint64(math.MaxInt64), "integer", "9223372036854775807"},
		{"float", 2.5, "real", "2.5"},
		{"bool", true, "integer", "1"},
		{"bytes", []byte("ab"), "blob", "ab"},
		{"empty bytes", []byte{}, "blob", ""},
		{"raw json", json.RawMessage(`{"a":1}`), "blob", `{"a":1}`},
		{"time", ts, "integer", "1700000000"},
		{"map", map[string]int{"a": 1}, "text", "{\"a\":1}\n"},
		{"slice", []int{1, 2}, "text", "[1,2]\n"},
		{"struct", payload{A: 1}, "text", "{\"a\":1}\n"},
		{"*struct", &payload{A: 2}, "text", "{\"a\":2}\n"},
		{"*string", &s, "text", "hi"},
		{"*int64", &n, "integer", "42"},
		{"*float64", &f, "real", "1.5"},
		{"*bool", &b, "integer", "1"},
		{"*uint32", &u, "integer", "7"},
		{"*time", &ts, "integer", "1700000000"},
		{"nil *string", nilStr, "null", ""},
		{"nil *struct", nilStruct, "null", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ty, v := bindRoundTrip(t, conn, tc.value)
			assert.Equal(t, tc.wantTy, ty)
			assert.Equal(t, tc.wantV, v)
		})
	}
}

func TestBindUint64Overflow(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()

	_, err = conn.Prepare(ctx, "SELECT ?;", uint64(math.MaxInt64)+1)
	assert.True(t, errors.Is(err, sqlite.ErrIntegerOverflow), "got %v", err)
}

func TestBindUnknownType(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()

	_, err = conn.Prepare(ctx, "SELECT ?;", make(chan int))
	assert.ErrorIs(t, err, sqlite.ErrUnknownType)
}

func TestPrepareTransient(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()

	stmt, err := conn.PrepareTransient(ctx, "SELECT ? + ? AS s;", 1, 2)
	require.NoError(t, err)
	hasRow, err := stmt.Step()
	require.NoError(t, err)
	require.True(t, hasRow)
	assert.Equal(t, int64(3), stmt.GetInt64("s"))
	require.NoError(t, stmt.Finalize())

	_, err = conn.PrepareTransient(ctx, "SELECT 1; SELECT 2;")
	assert.ErrorIs(t, err, sqlite.ErrPrepareSQL)

	_, err = conn.PrepareTransient(ctx, "SELECT ?;", make(chan int))
	assert.ErrorIs(t, err, sqlite.ErrUnknownType)
}

func TestBindNilByteSliceIsEmptyBlob(t *testing.T) {
	ctx := context.Background()
	db := newFileDB(t)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Done()
	var nilBytes []byte
	var nilRaw json.RawMessage
	ty, _ := bindRoundTrip(t, conn, nilBytes)
	assert.Equal(t, "blob", ty)
	ty, _ = bindRoundTrip(t, conn, nilRaw)
	assert.Equal(t, "blob", ty)
	type namedBytes []byte
	ty, v := bindRoundTrip(t, conn, namedBytes("nb"))
	assert.Equal(t, "blob", ty)
	assert.Equal(t, "nb", v)
}
