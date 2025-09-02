# run-sql-connectorx

An MCP server that executes SQL via **ConnectorX** and streams the result to **CSV** or **Parquet** in 
PyArrow `RecordBatch` chunks.

* **Output formats**: `csv` or `parquet`
* **CSV**: UTF-8, header row is always written
* **Parquet**: PyArrow defaults; schema mismatch across batches raises an error
* **Return value**: the string `"OK"` on success, or `"Error: <message>"` on failure
* On failure the partially written output file is deleted

## Why this library?

- **Efficient streaming**: handles large results in Arrow `RecordBatch` chunks
- **Token-efficient for MCP**: exchanges data via files instead of inline payloads
- **Cross-database via ConnectorX**: one tool works across many backends
- **Robust I/O**: CSV header handling, Parquet schema validation, safe cleanup on errors

## Supported data sources (ConnectorX)

ConnectorX supports many databases. Common examples include:

- **PostgreSQL**
- **MySQL / MariaDB**
- **SQLite**
- **Microsoft SQL Server**
- **Amazon Redshift**
- **Google BigQuery**

For the complete and up-to-date list of supported databases and connection-token (DSN) formats, see the official docs:

- ConnectorX repository: <https://github.com/sfu-db/connector-x/>
- Database connection tokens: <https://github.com/sfu-db/connector-x/tree/main/docs/databases>

## Getting Started

```bash
uvx run-sql-connectorx \
  --conn "<connection_token>"
```

<connection_token> is the **connection token** (*conn*) used by ConnectorX—SQLite, PostgreSQL, BigQuery, and more.

### Further reading

* ConnectorX repository: <https://github.com/sfu-db/connector-x/>
* Connection-token formats for each database: <https://github.com/sfu-db/connector-x/tree/main/docs/databases>

### Running from *mcp.json*

To launch the server from an MCP-aware client such as **Cursor**, add the following snippet to
`.cursor/mcp.json` at the project root:

```json
{
  "mcpServers": {
    "run-sql-connectorx": {
      "command": "uvx",
      "args": [
        "--from", "git+https://github.com/gigamori/mcp-run-sql-connectorx",
        "run-sql-connectorx",
        "--conn", "<connection_token>"
      ]
    }
  }
}
```

## Behaviour and Limits

* **Streaming**: Results are streamed from ConnectorX in RecordBatch chunks; the default
  `batch_size` is `100 000` rows.
* **Empty result**:
  * CSV – an empty file is created (with header row)
  * Parquet – an empty table is written
* **Error handling**: the output file is removed on any exception.

## MCP Tool Specification

The server exposes a single MCP tool **`run_sql`**.

| Argument        | Type   | Required | Description                    |
|-----------------|--------|----------|--------------------------------|
| `sql_file`      | string | yes      | Path to a file that contains the SQL text to execute |
| `output_path`   | string | yes      | Destination file for the query result |
| `output_format` | enum   | yes      | One of `"csv"` or `"parquet"`   |
| `batch_size`    | int    | no       | RecordBatch size (default `100000`) |

### Example Call

```json
{
  "tool": "run_sql",
  "arguments": {
    "sql_file": "sql/queries/sales.sql",
    "output_path": "output/sales.parquet",
    "output_format": "parquet",
    "batch_size": 200000
  }
}
```

---

## License

Distributed under the MIT License. See `LICENSE` for details.
