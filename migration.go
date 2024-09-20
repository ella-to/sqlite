package sqlite

import (
	"context"
	"io/fs"
	"path/filepath"
	"slices"
	"sort"

	"ella.to/logger"
)

type ReadDirFileFS interface {
	fs.ReadDirFS
	fs.ReadFileFS
}

// migration calls read each sql files in the migration directory and applies it to the database.
// It will create a table called migrations_sqlite to keep track of the files that have been applied.
//
// Use this function to apply migrations to the database at the start of your application.
// Make sure each file name is unique and the use either a timestamp or counter to make sure
// the files are applied in the correct order.
func Migration(ctx context.Context, db *Database, fs ReadDirFileFS, dir string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Done()

	sqlFiles, err := sortMigrationFiles(fs, dir)
	if err != nil {
		return err
	}

	err = createMigrationTable(ctx, conn)
	if err != nil {
		return err
	}

	alreadyMigratedFiles, err := loadAlreadyMigratedFiles(ctx, conn)
	if err != nil {
		return err
	}

	missingMigrations := detectMissingMigrations(alreadyMigratedFiles, sqlFiles)

	for _, sqlFile := range missingMigrations {
		logger.Debug(ctx, "running migration sql", "file", sqlFile)

		err = setMigrateFile(ctx, conn, sqlFile, fs)
		if err != nil {
			return err
		}
	}

	return nil
}

func detectMissingMigrations(alreadyMigratedFiles, sqlFiles []string) []string {
	var missingMigrations []string

	for _, sqlFile := range sqlFiles {
		if !slices.Contains(alreadyMigratedFiles, sqlFile) {
			missingMigrations = append(missingMigrations, sqlFile)
		}
	}

	return missingMigrations
}

func loadAlreadyMigratedFiles(ctx context.Context, conn *Conn) ([]string, error) {
	stmt, err := conn.Prepare(ctx, `SELECT filename FROM migrations_sqlite;`)
	if err != nil {
		return nil, err
	}

	var filenames []string
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}

		if !hasRow {
			break
		}

		filenames = append(filenames, stmt.GetText("filename"))
	}

	return filenames, nil
}

func setMigrateFile(ctx context.Context, conn *Conn, filename string, fs ReadDirFileFS) (err error) {
	defer conn.Save(&err)

	err = func() error {
		stmt, err := conn.Prepare(ctx, `INSERT INTO migrations_sqlite (filename) VALUES (?);`, filename)
		if err != nil {
			return err
		}

		_, err = stmt.Step()
		if err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		return err
	}

	content, err := fs.ReadFile(filename)
	if err != nil {
		return err
	}

	err = conn.ExecScript(string(content))
	if err != nil {
		return err
	}

	return nil
}

func createMigrationTable(ctx context.Context, conn *Conn) (err error) {
	defer conn.Save(&err)

	stmt, err := conn.Prepare(ctx, `CREATE TABLE IF NOT EXISTS migrations_sqlite (filename TEXT PRIMARY KEY);`)
	if err != nil {
		return err
	}

	_, err = stmt.Step()
	if err != nil {
		return err
	}

	return nil
}

func sortMigrationFiles(fs ReadDirFileFS, dir string) ([]string, error) {
	var sqlFiles []string

	files, err := fs.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if filepath.Ext(file.Name()) != ".sql" {
			continue
		}

		sqlFiles = append(sqlFiles, filepath.Join(dir, file.Name()))
	}

	sort.Strings(sqlFiles)

	return sqlFiles, nil
}
