package sqlite

import (
	"context"
	"io/fs"
	"log/slog"
	"path/filepath"
	"sort"
)

type ReadDirFileFS interface {
	fs.ReadDirFS
	fs.ReadFileFS
}

// migration calls read each sql files in the migration directory and applies it to the database.
func Migration(ctx context.Context, db *Database, fs ReadDirFileFS, dir string) error {
	var sqlFiles []string

	files, err := fs.ReadDir(dir)
	if err != nil {
		return err
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

	for _, sqlFile := range sqlFiles {
		slog.Debug("running migration sql", "file", sqlFile)

		content, err := fs.ReadFile(sqlFile)
		if err != nil {
			return err
		}

		err = RunScript(ctx, db, string(content))
		if err != nil {
			return err
		}
	}

	return nil
}
