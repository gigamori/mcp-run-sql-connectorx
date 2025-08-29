# run-sql-connectorx

ConnectorX を使用して任意の DSN に対して SQL を実行し、その結果を PyArrow RecordBatch 単位で CSV または Parquet にストリーミング書き込みする MCP サーバです。

* **出力形式**: `csv` または `parquet`
* **CSV**: UTF-8、ヘッダ行は常に出力
* **Parquet**: PyArrow 既定値。バッチ間でスキーマが一致しない場合はエラー
* **返却値**: 成功時は文字列 `"OK"`、失敗時は `"Error: <message>"`
* 失敗時は作成中の出力ファイルを削除します

## 起動方法

```bash
uvx run-sql-connectorx \
  --dsn "<connectorx_dsn>"
```

### mcp.json から起動する場合

MCP 対応クライアント（例: Cursor）から本サーバを起動するには、プロジェクト直下の `.cursor/mcp.json` に以下を追加します。

```json
{
  "mcpServers": {
    "run-sql-connectorx": {
      "command": "uvx",
      "args": [
        "--from", "git+https://github.com/gigamori/mcp-run-sql-connectorx",
        "run-sql-connectorx",
        "--dsn", "<connectorx_dsn>"
      ]
    }
  }
}
```

## 挙動と制限

* **ストリーミング**: ConnectorX から RecordBatch 単位で結果を取得し、既定 `batch_size` は `100 000` 行です。
* **空結果**:
  * CSV – ヘッダ行のみ（空ファイル）
  * Parquet – 空のテーブルを書き込み
* **エラー処理**: 例外発生時には出力ファイルを削除します。

## MCP ツール仕様

ツール名: **`run_sql`**

| 引数 | 型 | 必須 | 説明 |
|------|----|------|------|
| `sql_file` | string | yes | 実行する SQL を含むファイルパス |
| `output_path` | string | yes | 出力ファイルパス |
| `output_format` | enum | yes | `"csv"` または `"parquet"` |
| `batch_size` | int | no | RecordBatch サイズ（既定 `100000`） |

### 使用例

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

## ライセンス

MIT License
