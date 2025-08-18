# LogTapestry

## Compactor Usage

```
LogTapestry.Compactor.exe --data <path_to_data_dir> [--compact-older-than <timespan>]
```
- `--data` (required): Root directory of the Parquet data store.
- `--compact-older-than` (optional): TimeSpan string (e.g., "1h", "2d"). Defaults to "1h".

## Query Tool Usage

```
LogTapestry.Query.exe --database-path <sqlite_db_path> --data-path <parquet_data_dir> [--output <table|json|csv>] "SQL_QUERY"
```
- `--database-path` (required): Path to SQLite database.
- `--data-path` (required): Path to Parquet data directory.
- `--output` (optional): Output format (`table`, `json`, `csv`). Default is `table`.
- `SQL_QUERY`: The SQL query string.

## Example

```
LogTapestry.Query.exe --database-path ./state.db --data-path ./data --output json "SELECT user_id, session_id FROM logs WHERE user_id > 100"
```

## End-to-End Workflow

1. Start Ingester to ingest logs.
2. Run Compactor to optimize Parquet files.
3. Use Query tool to query data.

See integration tests for automated validation of the full data lifecycle.
