```
░██████╗░██████╗░██╗░░░░░██╗████████╗███████╗
██╔════╝██╔═══██╗██║░░░░░██║╚══██╔══╝██╔════╝
╚█████╗░██║██╗██║██║░░░░░██║░░░██║░░░█████╗░░
░╚═══██╗╚██████╔╝██║░░░░░██║░░░██║░░░██╔══╝░░
██████╔╝░╚═██╔═╝░███████╗██║░░░██║░░░███████╗
╚═════╝░░░░╚═╝░░░╚══════╝╚═╝░░░╚═╝░░░╚══════╝
```
<div align="center">

[![Go Reference](https://pkg.go.dev/badge/ella.to/sqlite.svg)](https://pkg.go.dev/ella.to/sqlite)
[![Go Report Card](https://goreportcard.com/badge/ella.to/sqlite)](https://goreportcard.com/report/ella.to/sqlite)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**sqlite** wraps [zombiezen.com/go/sqlite](https://pkg.go.dev/zombiezen.com/go/sqlite) with connection pooling, automatic parameter binding, migrations, and geo queries.

</div>

## Installation

```bash
go get ella.to/sqlite
```

## Opening a Database

```go
db, err := sqlite.New(ctx,
    sqlite.WithFile("app.db"),
    sqlite.WithPoolSize(10),
)
defer db.Close()
```

Every new connection gets these PRAGMAs applied automatically:

- `foreign_keys = ON`
- `journal_mode = WAL`
- `cache_size = -2000` (2 MB)
- `temp_store = MEMORY`

### In-Memory Database

```go
db, err := sqlite.New(ctx, sqlite.WithMemory())
```

### Custom Connection String

```go
db, err := sqlite.New(ctx, sqlite.WithStringConn("file:app.db?mode=ro"))
```

### Connection Preparation

Run setup logic on every new connection in the pool (e.g. loading extensions or registering custom functions):

```go
db, err := sqlite.New(ctx,
    sqlite.WithFile("app.db"),
    sqlite.WithConnPrepareFunc(func(conn *sqlite.Conn) error {
        return conn.ExecScript(`PRAGMA busy_timeout = 5000;`)
    }),
    sqlite.WithFunctions(map[string]*sqlite.FunctionImpl{
        "my_upper": {
            NArgs:         1,
            Deterministic: true,
            Scalar: func(ctx *sqlite.Context, args []sqlite.Value) {
                ctx.ResultText(strings.ToUpper(args[0].Text()))
            },
        },
    }),
)
```

## Running Queries

### Get a Connection

```go
conn, err := db.Conn(ctx)
defer conn.Done() // always return to pool
```

### Prepare and Execute

`Prepare` auto-binds parameters by position. It understands Go types and maps them to SQLite types:

```go
stmt, err := conn.Prepare(ctx, "SELECT * FROM users WHERE id = ? AND active = ?", userID, true)
```

| Go Type | SQLite Type |
|---------|-------------|
| `string`, `fmt.Stringer` | TEXT |
| `int`, `int64`, etc. | INTEGER |
| `float32`, `float64` | FLOAT |
| `bool` | INTEGER (0/1) |
| `[]byte`, `json.RawMessage` | BLOB |
| `time.Time` | INTEGER (Unix timestamp) |
| `nil` | NULL |
| maps, slices, structs | TEXT (JSON-encoded) |

### Using the Exec Helper

For one-off operations where you don't need to manage the connection yourself:

```go
err := db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
    stmt, err := conn.Prepare(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", name, email)
    if err != nil {
        return err
    }
    _, err = stmt.Step()
    return err
})
```

### Transactions

Use `Save` with a deferred call for automatic commit/rollback based on error:

```go
err := db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
    defer conn.Save(&err)

    stmt, _ := conn.Prepare(ctx, "INSERT INTO orders (user_id, total) VALUES (?, ?)", userID, total)
    stmt.Step()

    stmt, _ = conn.Prepare(ctx, "UPDATE users SET order_count = order_count + 1 WHERE id = ?", userID)
    stmt.Step()

    return nil // commits on nil, rolls back on error
})
```

### Running SQL Scripts

```go
// Execute a multi-statement string
err := sqlite.RunScript(ctx, db, `
    CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT);
    CREATE INDEX IF NOT EXISTS idx_users_name ON users(name);
`)

// Execute all .sql files from a directory (sorted alphabetically)
err := sqlite.RunScriptFiles(ctx, db, "./sql/setup/")
```

## Migrations

Apply SQL migration files from an embedded filesystem. Already-applied migrations are tracked in a `migrations_sqlite` table, so only new files run on each call.

```go
//go:embed migrations/*.sql
var migrationsFS embed.FS

err := sqlite.Migration(ctx, db, migrationsFS, "migrations")
```

Migration files are applied in sorted order. Name them with a numeric prefix to control ordering:

```
migrations/
  001_create_users.sql
  002_add_email_index.sql
  003_create_orders.sql
```

## Reading Columns

Helper functions for extracting typed values from query results:

```go
// Unix timestamp -> time.Time (UTC)
created := sqlite.LoadTime(stmt, "created_at")

// Integer -> bool
active := sqlite.LoadBool(stmt, "is_active")

// JSON column -> Go types
attrs, err := sqlite.LoadJsonMap[string](stmt, "attributes")
tags, err := sqlite.LoadJsonArray[string](stmt, "tags")
profile, err := sqlite.LoadJsonStruct[UserProfile](stmt, "profile")
```

## Placeholders

Generate placeholder strings for building dynamic queries:

```go
// Single row: "?, ?, ?"
sqlite.Placeholders(3)

// Multiple rows: "(?, ?), (?, ?), (?, ?)"
sqlite.GroupPlaceholders(3, 2)
```

## Geo Queries

Built-in support for bounding-box distance queries using the haversine formula. Useful for "find things near this point" queries.

```go
// WHERE clause that filters to a bounding box around the point
where := sqlite.CreateCondSQL(lat, lon, distanceInKm)

// SELECT expression that computes squared distance from the point
dist := sqlite.CreateDistanceSQL(lat, lon)

query := fmt.Sprintf(
    "SELECT *, %s AS dist FROM locations WHERE %s ORDER BY dist LIMIT 20",
    dist, where,
)
```

The bounding box uses a 1.1x safety margin, so you get slightly more results than the exact radius — good for a first pass before doing precise distance filtering in your application.

## Debugging

Format a query with its parameters for logging:

```go
sql := sqlite.ShowSql("SELECT * FROM users WHERE name = ? AND age > ?", "Alice", 30)
// SELECT * FROM users WHERE name = 'Alice' AND age > 30
```

## Error Sentinels

```go
sqlite.ErrNotFound    // row not found
sqlite.ErrPrepareSQL  // failed to prepare statement
sqlite.ErrExecSQL     // failed to execute statement
sqlite.ErrUnknownType // unsupported Go type for parameter binding
```

## License

MIT — see [LICENSE.md](LICENSE.md) for details.