from __future__ import annotations

import sys
import os
import argparse
from pathlib import Path
# Standard library
from typing import Iterator, Optional
import io
import threading

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
  parser.add_argument(
    "--csv-token-threshold",
    type=int,
    default=0,
    help="When >0, count CSV tokens using tiktoken(o200k_base) and report total; value acts as a warning threshold",
  )

  return parser.parse_args()


# -------- batch writers --------

def _write_csv_batches(
  header: list[str],
  batches: Iterator[pa.RecordBatch],
  output_path: Path,
  encoder: Optional[object] = None,
) -> int:
  """Write CSV in streaming fashion; optionally count tokens of each written line.

  When an encoder is provided (tiktoken Encoding), count tokens for each formatted CSV line
  including delimiters, quotes, and line terminators as produced by csv.writer.
  """
  tokens_total = 0
  header_written = output_path.exists() and output_path.stat().st_size > 0

  def _line_tokens(values: list[object]) -> int:
    if encoder is None:
      return 0
    buf = io.StringIO(newline="")
    tmp_writer = csv.writer(buf)
    tmp_writer.writerow(values)
    line = buf.getvalue()
    # Lazy import typing for type hints avoided; encoder is a tiktoken Encoding-like object
    return len(encoder.encode(line))  # type: ignore[attr-defined]

  with output_path.open("a", newline="", encoding="utf-8") as fp:
    writer = csv.writer(fp)
    for batch in batches:
      table = pa.Table.from_batches([batch])
      if not header_written:
        writer.writerow([col for col in table.schema.names])
        tokens_total += _line_tokens([col for col in table.schema.names])
        header_written = True
      for row_idx in range(table.num_rows):
        row = [table.column(col_idx)[row_idx].as_py() for col_idx in range(table.num_columns)]
        writer.writerow(row)
        tokens_total += _line_tokens(row)
  return tokens_total


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
  """Re-raise ConnectorX errors as RuntimeError without altering the original message."""
  raise RuntimeError(error_msg)


# -------- native stderr capture (to collect Rust panic output) --------

class _StderrCapture:
  def __init__(self) -> None:
    self._orig_fd: Optional[int] = None
    self._read_fd: Optional[int] = None
    self._write_fd: Optional[int] = None
    self._buffer: list[bytes] = []
    self._thread: Optional[threading.Thread] = None
    self._stop_event = threading.Event()

  def __enter__(self) -> "_StderrCapture":
    self._orig_fd = os.dup(2)
    self._read_fd, self._write_fd = os.pipe()
    os.dup2(self._write_fd, 2)

    def _reader() -> None:
      try:
        while not self._stop_event.is_set():
          try:
            chunk = os.read(self._read_fd, 4096)  # type: ignore[arg-type]
          except OSError:
            break
          if not chunk:
            break
          self._buffer.append(chunk)
      finally:
        pass

    self._thread = threading.Thread(target=_reader, daemon=True)
    self._thread.start()
    return self

  def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
    try:
      if self._write_fd is not None:
        try:
          os.close(self._write_fd)
        except OSError:
          pass
      self._stop_event.set()
      if self._thread is not None:
        self._thread.join(timeout=0.5)
      if self._orig_fd is not None:
        try:
          os.dup2(self._orig_fd, 2)
        finally:
          try:
            os.close(self._orig_fd)
          except OSError:
            pass
      if self._read_fd is not None:
        try:
          os.close(self._read_fd)
        except OSError:
          pass
    finally:
      self._orig_fd = None
      self._read_fd = None
      self._write_fd = None
      self._thread = None

  def get_text(self) -> str:
    if not self._buffer:
      return ""
    try:
      return b"".join(self._buffer).decode("utf-8", errors="replace")
    except Exception:
      return ""


# -------- record batch iterator via ConnectorX --------

def _iter_record_batches(conn: str, sql_text: str, batch_size: int) -> Iterator[pa.RecordBatch]:
  import connectorx as cx  # type: ignore
  from urllib.parse import urlparse
  
  # Determine database type from the connection token scheme
  parsed_conn = urlparse(conn)
  db_type = (parsed_conn.scheme or "").lower()
  
  # Always use arrow_stream as requested; capture Rust panic output written to stderr
  with _StderrCapture() as cap:
    try:
      stream = cx.read_sql(conn, sql_text, return_type="arrow_stream", batch_size=batch_size)
    except BaseException as e:  # includes PanicException
      msg = str(e)
      stderr_txt = cap.get_text()
      if stderr_txt:
        msg = f"{msg}\n{stderr_txt}"
      _handle_connectorx_error(msg, db_type)
  
  with _StderrCapture() as cap:
    try:
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
    except BaseException as e:  # includes PanicException during iteration
      msg = str(e)
      stderr_txt = cap.get_text()
      if stderr_txt:
        msg = f"{msg}\n{stderr_txt}"
      _handle_connectorx_error(msg, db_type)

# -------- MCP server --------

def run_server(conn: str, csv_token_threshold: int = 0) -> None:
  import asyncio
  from mcp.server import Server  # type: ignore
  import mcp.server.stdio  # type: ignore
  import mcp.types as types  # type: ignore
  from mcp.server.models import InitializationOptions  # type: ignore
  from mcp.server import NotificationOptions  # type: ignore

  server = Server("run-sql-connectorx")
  run_server._conn = conn  # type: ignore
  run_server._csv_token_threshold = int(csv_token_threshold)  # type: ignore
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
        # Always write the header row when there is at least one batch
        first = next(batches, None)
        threshold = max(0, int(getattr(run_server, "_csv_token_threshold", 0)))  # type: ignore
        if first is None:
          # Empty result set: create an empty file (no column info available)
          with out.open("w", newline="", encoding="utf-8") as fp:
            pass
          if threshold > 0:
            return [types.TextContent(type="text", text="OK 0 tokens")]
        else:
          header = [f.name for f in first.schema]
          def chain_batches():
            yield first
            for b in batches:
              yield b
          if threshold > 0:
            import tiktoken  # type: ignore
            encoder = tiktoken.get_encoding("o200k_base")
            total_tokens = _write_csv_batches(header, chain_batches(), out, encoder=encoder)
            if total_tokens >= threshold:
              return [types.TextContent(type="text", text=f"OK {total_tokens} tokens. Too many tokens may impair processing. Handle appropriately")]
            else:
              return [types.TextContent(type="text", text=f"OK {total_tokens} tokens")]
          else:
            _write_csv_batches(header, chain_batches(), out, encoder=None)
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
    except BaseException as e:
      # Cleanup any partial output on failure
      try:
        if "out" in locals() and isinstance(out, Path) and out.exists():
          out.unlink()
      except Exception:
        pass
      
      # Build a detailed error message based on the error category
      # Always return the original message as-is for maximum transparency and future-proofing
      detailed_msg = str(e)
      
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
            server_version="0.1.1",
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
    run_server(conn=args.conn, csv_token_threshold=getattr(args, "csv_token_threshold", 0))
  except Exception as exc:
    print(f"Error: {exc}", file=sys.stderr)
    print(f"\n{DOC_USAGE}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
  main()


