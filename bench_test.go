package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"ella.to/sqlite"
)

func BenchmarkPrepareBind(b *testing.B) {
	ctx := context.Background()
	db, err := sqlite.New(ctx, sqlite.WithFile(filepath.Join(b.TempDir(), "b.db")), sqlite.WithPoolSize(1))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Done()

	now := time.Now()
	blob := []byte("blob")
	b.ReportAllocs()
	for b.Loop() {
		stmt, err := conn.Prepare(ctx, "SELECT ?, ?, ?, ?, ?, ?, ?;", "text", 42, int64(7), 1.5, true, now, blob)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := stmt.Step(); err != nil {
			b.Fatal(err)
		}
		if err := stmt.Reset(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnTakePut(b *testing.B) {
	ctx := context.Background()
	db, err := sqlite.New(ctx, sqlite.WithMemory(), sqlite.WithPoolSize(1))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ReportAllocs()
	for b.Loop() {
		conn, err := db.Conn(ctx)
		if err != nil {
			b.Fatal(err)
		}
		conn.Done()
	}
}
