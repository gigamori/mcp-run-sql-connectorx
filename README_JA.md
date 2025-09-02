# run-sql-connectorx

ConnectorX を使用して任意の DSN に対して SQL を実行し、その結果を PyArrow RecordBatch 単位で CSV または Parquet にストリーミング書き込みする MCP サーバです。

* **出力形式**: `csv` または `parquet`
* **CSV**: UTF-8、ヘッダ行は常に出力
* **Parquet**: PyArrow 既定値。バッチ間でスキーマが一致しない場合はエラー
* **返却値**: 成功時は文字列 `"OK"`、失敗時は `"Error: <message>"`
* 失敗時は作成中の出力ファイルを削除します

## このライブラリの狙い

- **大規模データに強い**: Arrow の `RecordBatch` 単位でストリーミング処理
- **MCP でトークン効率が良い**: データはインラインではなくファイルで受け渡し
- **ConnectorX による多種 DB 対応**: ひとつのツールで複数バックエンドに対応
- **堅牢な入出力**: CSV ヘッダ出力、Parquet スキーマ検証、例外時の安全な後片付け

## ConnectorX の対応データソース

代表的な対応先は以下の通りです。

- **PostgreSQL**
- **MySQL / MariaDB**
- **SQLite**
- **Microsoft SQL Server**
- **Amazon Redshift**
- **Google BigQuery**

最新かつ網羅的な対応一覧と接続トークン（conn）形式は公式ドキュメントを参照してください。

- ConnectorX 公式リポジトリ: <https://github.com/sfu-db/connector-x/>
- 各データベースの接続トークン一覧: <https://github.com/sfu-db/connector-x/tree/main/docs/databases>

## 起動方法

```bash
uvx run-sql-connectorx \
  --conn "<connection_token>"
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
        "--conn", "<connection_token>"
      ]
    }
  }
}
```

## 挙動と制限

* **ストリーミング**: ConnectorX から RecordBatch 単位で結果を取得（既定 `batch_size` = 100&nbsp;000 行）
* **空結果**:
  * CSV – ヘッダ行のみの空ファイルを作成
  * Parquet – 空のテーブルを書き込み
* **エラー処理**: 例外発生時には出力ファイルを削除します。

## MCP ツール仕様

公開ツール: **`run_sql`**

| 引数 | 型 | 必須 | 説明 |
|------|----|------|------|
| `sql_file` | string | ◯ | 実行する SQL を含むファイルパス |
| `output_path` | string | ◯ | 結果を書き込むファイルパス |
| `output_format` | enum | ◯ | `"csv"` または `"parquet"` |
| `batch_size` | int | – | RecordBatch サイズ（既定 100&nbsp;000） |

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

本プロジェクトは MIT License の下で配布されています。詳細は `LICENSE` を参照してください。