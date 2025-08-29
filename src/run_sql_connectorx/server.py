from __future__ import annotations

import sys
import argparse
from pathlib import Path
# Standard library
from typing import Iterator, Optional

# Third-party libraries
import csv

import pyarrow as pa
import pyarrow.parquet as pq


DOC_USAGE = """
MCP Server: ConnectorX → CSV/Parquet (RecordBatch streaming)

Purpose:
  Provide an MCP tool (run_sql) that executes an arbitrary SQL statement via ConnectorX
  and streams the result to CSV or Parquet in RecordBatch chunks.

How to start the server:
  uvx run-sql-connectorx \
    --conn <connection_token>

Exposed MCP tool: run_sql
  Parameters:
    sql_file (str)       – Path to a file that contains the SQL to execute
    output_path (str)    – Destination file path for the result
    output_format (str)  – Either "csv" or "parquet"
    batch_size (int)     – Optional RecordBatch size (default 100000)
"""


# -------- CLI / server bootstrap --------

def _parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(description="MCP Server: ConnectorX -> CSV/Parquet (RecordBatch streaming)")

  parser.add_argument("--conn", required=True, help="ConnectorX connection token (server-wide, fixed)")

  return parser.parse_args()


# -------- batch writers --------

def _write_csv_batches(header: list[str], batches: Iterator[pa.RecordBatch], output_path: Path) -> None:
  header_written = output_path.exists() and output_path.stat().st_size > 0
  with output_path.open("a", newline="", encoding="utf-8") as fp:
    writer = csv.writer(fp)
    for batch in batches:
      table = pa.Table.from_batches([batch])
      if not header_written:
        writer.writerow([col for col in table.schema.names])
        header_written = True
      for row_idx in range(table.num_rows):
        row = [table.column(col_idx)[row_idx].as_py() for col_idx in range(table.num_columns)]
        writer.writerow(row)


def _write_parquet_batches(batches: Iterator[pa.RecordBatch], output_path: Path) -> None:
  writer: Optional[pq.ParquetWriter] = None
  try:
    for batch in batches:
      table = pa.Table.from_batches([batch])
      if writer is None:
        writer = pq.ParquetWriter(where=str(output_path), schema=table.schema)
      else:
        if not table.schema.equals(writer.schema):
          raise RuntimeError("Schema mismatch across record batches")
      writer.write_table(table)
  finally:
    if writer is not None:
      writer.close()


# -------- shared error handling --------

def _handle_connectorx_error(error_msg: str, vendor: str) -> None:
  """Re-raise ConnectorX errors as RuntimeError with a consistent message."""
  raise RuntimeError(f"Database connection failed ({vendor}): {error_msg}")


# -------- record batch iterator via ConnectorX --------

def _iter_record_batches(conn: str, sql_text: str, batch_size: int) -> Iterator[pa.RecordBatch]:
  import connectorx as cx  # type: ignore
  from urllib.parse import urlparse
  
  # Determine database type from the connection token scheme
  parsed_conn = urlparse(conn)
  db_type = parsed_conn.scheme
  
  # Re-raise using the shared error-handling helper
  try:
    stream = cx.read_sql(conn, sql_text, return_type="arrow_stream", batch_size=batch_size)
  except Exception as e:
    _handle_connectorx_error(str(e), db_type)
  
  # Stream the resulting record batches
  for rb in stream:  # type: ignore
    if not isinstance(rb, pa.RecordBatch):
      # Some ConnectorX back-ends may yield a pyarrow.Table instead; split into batches
      if isinstance(rb, pa.Table):
        for b in rb.to_batches(max_chunksize=batch_size):
          yield b
      else:
        raise RuntimeError("ConnectorX did not return RecordBatch or Table stream")
    else:
      yield rb

# -------- MCP server --------

def run_server(conn: str) -> None:
  import asyncio
  from mcp.server import Server  # type: ignore
  import mcp.server.stdio  # type: ignore
  import mcp.types as types  # type: ignore
  from mcp.server.models import InitializationOptions  # type: ignore
  from mcp.server import NotificationOptions  # type: ignore

  server = Server("run-sql-connectorx")
  run_server._conn = conn  # type: ignore
  run_server._eager_connect = True  # type: ignore  # Always perform an eager connection check

  def tool_spec():
    return types.Tool(
      name="run_sql",
      description="Execute SQL via ConnectorX and write to CSV/Parquet (token-efficient: data is exchanged via files, not inline).",
      inputSchema={
        "type": "object",
        "properties": {
          "sql_file": {"type": "string"},
          "output_path": {"type": "string"},
          "output_format": {"type": "string", "enum": ["csv", "parquet"]},
          "batch_size": {"type": "integer"},
        },
        "required": ["sql_file", "output_path", "output_format"],
      },
    )

  @server.list_tools()
  async def handle_list_tools() -> list[types.Tool]:
    return [tool_spec()]

  @server.call_tool()
  async def handle_call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
    if name != "run_sql":
      return [types.TextContent(type="text", text="Unknown tool")]
    if not arguments:
      return [types.TextContent(type="text", text="Missing arguments")]
    try:
      sql_file = arguments.get("sql_file")
      output_path = arguments.get("output_path")
      output_format = arguments.get("output_format")
      batch_size = int(arguments.get("batch_size", 100000))
      if not sql_file or not output_path or output_format not in ("csv", "parquet"):
        raise ValueError("invalid arguments")

      sql_text = Path(sql_file).read_text(encoding="utf-8")
      if not sql_text.strip():
        raise ValueError("sql is empty")

      out = Path(output_path)
      out.parent.mkdir(parents=True, exist_ok=True)
      if out.exists():
        out.unlink()

      # Retrieve data via ConnectorX
      batches = _iter_record_batches(run_server._conn, sql_text, batch_size)  # type: ignore
      
      if output_format == "csv":
        # Always write the header row
        first = next(batches, None)
        if first is None:
          # Empty result set: create an empty file (no column info available)
          with out.open("w", newline="", encoding="utf-8") as fp:
            pass
        else:
          header = [f.name for f in first.schema]
          def chain_batches():
            yield first
            for b in batches:
              yield b
          _write_csv_batches(header, chain_batches(), out)
      else:
        # Parquet: write an empty table when the result set is empty
        first = next(batches, None)
        if first is None:
          table = pa.table({})
          pq.write_table(table, out)
        else:
          def chain_batches():
            yield first
            for b in batches:
              yield b
          _write_parquet_batches(chain_batches(), out)

      return [types.TextContent(type="text", text="OK")]
    except Exception as e:
      # Cleanup any partial output on failure
      try:
        if "out" in locals() and isinstance(out, Path) and out.exists():
          out.unlink()
      except Exception:
        pass
      
      # Build a detailed error message based on the error category
      error_msg = str(e)
      
      # File-related errors
      if "sql is empty" in error_msg:
        detailed_msg = f"SQL file error: The SQL file '{sql_file}' is empty or contains only whitespace."
      elif "No such file" in error_msg or "FileNotFoundError" in error_msg:
        detailed_msg = f"File not found: SQL file '{sql_file}' does not exist. Check the file path."
      elif "PermissionError" in error_msg:
        detailed_msg = f"Permission denied: Cannot read SQL file '{sql_file}' or write to output directory."
      
      # Argument-related errors
      elif "invalid arguments" in error_msg:
        detailed_msg = f"Invalid arguments: Required parameters are sql_file, output_path, and output_format (csv or parquet)."
      
      # Database connection errors (already detailed)
      elif "BigQuery" in error_msg or "SQLite" in error_msg or "PostgreSQL" in error_msg or "MySQL" in error_msg:
        detailed_msg = error_msg
      
      # Fallback for any other error
      else:
        detailed_msg = f"Execution failed: {error_msg}"
      
      # Print traceback for debugging
      import traceback
      print(f"Full traceback:\n{traceback.format_exc()}", file=sys.stderr)
      
      return [types.TextContent(type="text", text=f"Error: {detailed_msg}")]

  def run_sync():
    async def run_async():
      async with mcp.server.stdio.stdio_server() as (r, w):
        await server.run(
          r,
          w,
          InitializationOptions(
            server_name="run-sql-connectorx",
            server_version="0.1.0",
            capabilities=server.get_capabilities(notification_options=NotificationOptions(), experimental_capabilities={}),
          ),
        )
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
      loop.run_until_complete(run_async())
    except Exception as e:
      import traceback, sys as _sys
      print(f"Server initialization error: {e}", file=_sys.stderr)
      traceback.print_exc()
      raise
    finally:
      loop.close()

  run_sync()


def _validate_connection(conn: str) -> None:
  """Validate the database connection at start-up.

  * BigQuery   – Basic connection-token checks and an actual connection test
  * SQLite     – Validate in-memory or file path usage
  * Others     – Basic connection-token structure check and connection test
  """
  from urllib.parse import urlparse, parse_qsl

  parsed = urlparse(conn)
  vendor = (parsed.scheme or "").lower()

  if not vendor:
    raise RuntimeError("Connection token error: missing scheme (e.g., postgresql://, bigquery://, sqlite://)")

  # BigQuery: verify that the path part contains a credentials file
  if vendor.startswith("bigquery"):
    # Ensure credentials path is provided
    if not parsed.path or parsed.path == "/":
      raise RuntimeError("BigQuery connection token error: missing authentication file path (format: bigquery:///path/to/auth.json)")
    return

  # SQLite: if it is a file database, ensure the parent directory exists (:memory: is allowed)
  if vendor.startswith("sqlite"):
    # Skip check for in-memory database (sqlite://:memory:)
    if parsed.netloc == ":memory:":
      return
    # For file databases, confirm the parent directory exists
    path = (parsed.path or "").lstrip("/")
    if path:
      parent = Path(path).expanduser().resolve().parent
      if not parent.exists():
        raise RuntimeError(f"SQLite path error: parent directory does not exist: {parent}")
    return

  # Other vendors: at minimum, ensure host info is present (no connection attempt yet)
  # e.g. postgresql://user:pass@host:5432/db
  if not (parsed.netloc or parsed.hostname):
    raise RuntimeError(f"Connection token error: missing host in token for vendor '{vendor}'")
  # Do not attempt a network connection here to avoid hangs

  # Perform a real connection test (common to all vendors)
  try:
    import connectorx as cx  # type: ignore
    result = cx.read_sql(conn, "SELECT 1", return_type="arrow")  # type: ignore
  except Exception as conn_exc:
    # Use the shared error handler
    _handle_connectorx_error(str(conn_exc), vendor)


def main() -> None:
  args = _parse_args()
  try:
    # Validate database connection before starting the MCP server
    _validate_connection(args.conn)
    run_server(conn=args.conn)
  except Exception as exc:
    print(f"Error: {exc}", file=sys.stderr)
    print(f"\n{DOC_USAGE}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
  main()


