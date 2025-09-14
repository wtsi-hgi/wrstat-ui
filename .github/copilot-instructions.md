# WRStat-UI AI Assistant Instructions

## Project Overview
WRStat-UI is a Go application providing user interfaces to a WRStat database. The codebase is split into several components:
- Core CLI app based on Cobra commands
- ClickHouse database integration for efficient data storage and querying
- Multiple web servers for different functionalities (main server, analytics, syscalls)
- React-based frontend for visualization

## Architecture

### Command Structure
- `cmd/` - Contains all CLI commands using Cobra framework
- `server/` - Web server implementation and API endpoints
- `analytics/` - Analytics server for viewing analytics data
- `syscalls/` - Syscall log analyzer server
- `basedirs/`, `db/`, `stats/`, etc. - Core business logic packages

### Data Flow
1. Stats files are ingested from filesystem scans
2. Data is summarized and stored in ClickHouse database
3. Web servers provide API access to the data
4. Frontend visualizes the data for users

## Development Workflows

### Building
```bash
# Full build including React frontend
make

# Build without npm dependencies (Go only)
make buildnonpm

# Compile embedded assets for analytics/syscalls
make buildembedded
```

### Testing
```bash
# Run all tests (avoid this for routine changes)
make test

# Run specific tests (e.g. ClickHouse integration tests)
# Always run these after changes to verify nothing is broken
go test . -run Click -v
go test ./clickhouse -v

# Test with race detection
make race
```

### Linting
```bash
# Lint everything including JavaScript
make lint

# Lint only Go code - always run this after making changes
# Do not ask for permission to run this command
make lintnonpm
```

## Project-Specific Conventions

### ClickHouse Integration
- Schema is created in `cmd/summarise.go` with specific ClickHouse SQL dialect
- Data ingestion happens in batch through the `ingestScan` function
- Functions use `context.Context` for cancellation and timeout support
- Query builders use raw SQL with parameterization via `?` placeholders

Example query pattern:
```go
err := conn.QueryRow(ctx, 
    "SELECT count() FROM scans WHERE state = ? AND mount_path = ?", 
    "ready", mountPath).Scan(&result)
```

### Error Handling
- Use `fmt.Errorf("context: %w", err)` for error wrapping
- Log errors with appropriate level via `appLogger`
- Critical errors use `die()` which logs and exits

### Frontend Integration
- Frontend code is in `server/static/wrstat/` (React)
- Analytics and syscalls UIs are embedded using the `embed.sh` script
- Build process compiles and embeds frontend into Go binary

## Integration Points

### ClickHouse Database
- Connection settings configurable via command-line flags
- Schema creation and migration handled in `createSchema` function
- Views and materialized views for optimized queries

### Authentication
- Uses `github.com/wtsi-hgi/go-authserver` for auth
- Middleware setup in `server/auth.go`

## Common Operations

### Adding a New Command
1. Create a new file in `cmd/` directory
2. Implement a Cobra command
3. Add it as a subcommand in `init()` of the file
4. Register to parent command (usually `RootCmd`)

### Working with ClickHouse
- Use the connection with proper context management
- Maintain backward compatibility with existing schema
- Test queries with various data sizes and filter conditions

### Optimizing Performance
- The codebase uses ClickHouse optimization techniques:
  - Materialized views for rollups
  - Batch inserts for better throughput
  - Path-specific indexes
- When adding features that query data, consider performance implications

## Testing Approach
- Do not "unit test" implementation details. Do TDD-style tests focusing on behavior of public functions.
- Integration tests with ClickHouse for database operations
- Special test setup in `main_test.go` for end-to-end testing
- **Do not run all tests during routine development; use targeted test patterns**

## Code Verification
When making changes, always verify your work by running:
1. `gopls format -w <changed_files>` - to format code before checking linting (no user approval needed)
2. `golangci-lint run --fix` - to auto-fix linting issues (no user approval needed)
2. `make lintnonpm` - to check for remaining linting issues (no user approval needed)
3. `CGO_ENABLED=1 go test -tags netgo --count 1 -v . -run TestClickHouse` - to verify ClickHouse integration (no user approval needed)
4. `CGO_ENABLED=1 go test -tags netgo --count 1 -v ./clickhouse` - to verify ClickHouse package (no user approval needed)

You can run these commands as needed without asking for permission.

## Linting Rules
Follow Go best practices and fix these common linting issues:
- `gocognit`: Cognitive complexity - break complex functions into smaller ones
- `gocyclo`: Cyclomatic complexity - simplify control flow
- `funlen`: Function length - keep functions focused and short
- `nlreturn`: Add blank line before returns
- `wsl_v5`: Whitespace linting rules
- `errcheck`: Always check errors from functions that return them