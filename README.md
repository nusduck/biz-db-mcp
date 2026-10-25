# biz-db-mcp

面向 UPRC 智能体平台的 MySQL MCP Server：按调用者机构隔离的只读查询、经 MCP 返回文件的导出，以及白名单内的产物导入。

> v0.3 平台接入：Streamable HTTP + Bearer、宿主参数 `instituteid`、表替换式行级隔离、DBPM 原生 TCP 取密、`config.yaml` 配置、导出改为 embedded resource、新增 `import_rows`。迁移说明见文末。

## 设计边界

- **机构隔离**：所有工具都有顶层必填字符串参数 `instituteid`，由平台按登录用户注入（模型看不到、改不了）。服务端按平台同一规则归一（`0800001000` → `00001000`），未知机构直接拒绝。
- **行级过滤**：模型写的 SELECT 用 sqlglot 解析，每个基表引用替换为 `(SELECT * FROM 表 WHERE 机构列 IN (...)) AS 别名`，对 JOIN、OR、外连接、聚合、子查询、UNION、CTE 同样成立。非公共表必须有机构列（`isolation.column_candidates` 中第一个存在的列）或显式配置，否则视为不存在。
- **SQL 准入**：只允许单条 SELECT（含 WITH / UNION）；含任何注释、锁定读、`INTO`、变量、未知/危险函数、系统库的语句一律拒绝。
- **只读**：读连接使用最小权限读账号并设置 `SESSION TRANSACTION READ ONLY`；行数、序列化字节、执行时间都有上限，截断会标明原因。
- **导出**：生成 Parquet/CSV/JSONL 后以 MCP embedded resource（base64）返回，平台写入会话工作区。本服务不调用 Sandbox、不持有 Sandbox 凭据。超限整体失败，不截断。
- **导入**：`import_rows` 默认关闭；只能写运维白名单内的表与列，机构列由服务端写入，单事务、持久幂等账本防重复写入。
- **通用 DML**：`execute` / `execute_many` 仅供本地 stdio 开发，HTTP 部署下拒绝启动。
- **审计**：stderr 输出 JSON 行，只含工具、机构码、库/表、SQL 摘要与长度、状态、错误码和耗时；不含 SQL 原文、参数、凭据、文件内容或结果数据。

## 目录结构

```text
biz_db_mcp/
  config.py        # config.yaml 加载与严格校验
  institution.py   # 机构号校验与归一
  policy.py        # 机构范围、表策略
  sql_guard.py     # SELECT 准入与表替换改写（另含开发用 DML 守卫）
  db.py            # 连接、元数据、隔离查询、导出流
  credentials.py   # 启动时一次性取齐数据库口令
  dbpm.py          # DBPM 原生 TCP 协议客户端
  export.py        # 固定 schema 的流式文件生成
  mcp_files.py     # 文件 → embedded resource
  import_rows.py   # 产物导入：校验、类型检查、事务写入
  import_ledger.py # 幂等账本
  limits.py        # 并发槽与机构速率
  transport.py     # HTTP Bearer 鉴权与请求体上限
  errors.py        # 稳定错误码
  audit.py         # 结构化审计日志
  server.py        # MCP 工具与启动入口
migrations/        # DBA 执行的账本 DDL 与授权模板
tests/
```

## 配置

全部可信配置放在一个 YAML 文件里，启动时加载一次，修改后重启生效。查找顺序：`--config` 参数 → `$BIZDB_CONFIG` → 当前目录 `config.yaml`。复制 [config.example.yaml](config.example.yaml) 为 `config.yaml`（已被 git 忽略）后修改。未知键会直接报错，避免拼写错误被悄悄忽略。

文件里**不放秘密**：

| 秘密 | 来源 |
|---|---|
| MCP Bearer token | `server.auth_token_env` 指定的环境变量（默认 `BIZDB_MCP_TOKEN`，平台 `authTokenRef` 用同名） |
| 数据库口令 | DBPM（`dbpm.enabled: true`），启动时取一次，只在内存中 |
| 本地开发口令 | 仅 stdio 且 DBPM 关闭时，`databases.<id>.password_env` 指定的环境变量 |

DSN 中带口令会被拒绝。主要配置块：

| 块 | 作用 |
|---|---|
| `server` | `transport`（stdio / streamable-http）、`host`、`port`、`auth_token_env`、`allowed_hosts`、`max_request_bytes`、`tool_deadline_seconds` |
| `limits` | query / export / import 的行数、字节、时间上限；`concurrency` 并发槽；`rate` 每机构速率。均为**每实例**预算 |
| `dbpm` | 两个 `host:port` 端点与连接/单端点/总预算超时 |
| `databases` | 连接目录：无口令 DSN、DBPM 条目名、`allow_reads`、`allow_imports`、独立 `import_credential`、按库覆盖的上限 |
| `institutions` | 归一机构码 → `scope: self / institutions / all`、`ids`、`allow_imports`。**键必须加引号**，否则 YAML 会丢前导零 |
| `isolation` | `column_candidates`、`public_tables`、按表显式 `tables`（`column`、`value_format: code / raw`） |
| `imports` | 总开关与白名单表：`modes`、`columns`、`key_columns`、`institution_column`、`max_rows` |
| `writes` | 仅本地 stdio 的通用 DML 开关 |

## 工具

| 工具 | 说明 |
|---|---|
| `list_databases` | 本机构可读的库 |
| `list_tables` | 本机构可读的表/视图，`pattern` 为 LIKE |
| `describe_table` | 列元数据 |
| `query` | 隔离后的只读 SELECT；`limit`/`offset` 分页，`dry_run` 只做校验与授权解析 |
| `explain` | 隔离后 SELECT 的执行计划 |
| `export` | 结果文件以 embedded resource 返回；超限报 `TOO_LARGE` |
| `import_rows` | `imports.enabled` 时注册；白名单产物导入，`dry_run` 默认 true |
| `execute` / `execute_many` | 仅本地 stdio 且 `writes.enabled` 时注册 |

错误统一为 `isError` 结果，文本以稳定错误码开头，如 `FORBIDDEN_INSTITUTION`、`RESOURCE_NOT_ACCESSIBLE`、`SQL_REJECTED`、`TABLE_NOT_IMPORTABLE`、`CONTENT_DIGEST_MISMATCH`、`TOO_LARGE`、`RATE_LIMITED`、`BUSY`、`DEADLINE_EXCEEDED`、`IMPORT_OUTCOME_UNKNOWN`。跨机构拒绝与“不存在”返回同一错误。

## 部署

```bash
python3 -m pip install -e '.[dev]'
python3 -m pytest
export BIZDB_MCP_TOKEN=...            # 只有平台持有
biz-db-mcp --config /etc/biz-db-mcp/config.yaml --transport streamable-http --host 0.0.0.0 --port 8080
```

端点为 `http://<host>:<port>/mcp`，无状态模式，便于多副本。容器以非 root 用户运行，经内网或 TLS 反向代理暴露。反向代理/网关的请求体上限至少为 `server.max_request_bytes`，响应上限需容纳 20 MiB 文件的 base64（约 27 MiB）加 JSON。服务端端到端期限（默认 150 秒）须低于平台 `timeoutMs`（示例 180 秒）。

平台清单示例：

```json
{
  "id": "bizdb",
  "url": "https://bizdb-mcp.internal.example/mcp",
  "authTokenRef": "BIZDB_MCP_TOKEN",
  "timeoutMs": 180000,
  "hostArguments": {"instituteid": {"description": "调用者机构号", "source": "user.instituteId"}},
  "artifactArguments": {
    "file_base64": {
      "description": "已提交的待导入数据产物",
      "modelArgument": "artifact_id",
      "filenameArgument": "filename",
      "sha256Argument": "content_sha256",
      "maxBytes": 10485760,
      "mimeTypes": ["text/csv", "application/x-ndjson", "application/vnd.apache.parquet"]
    }
  },
  "fileResults": {"maxBytes": 20971520}
}
```

启用导入前：DBA 执行 [migrations/001_import_ledger.sql](migrations/001_import_ledger.sql)，按其中模板给独立导入账号授权；核对目标表唯一约束含机构列。启动时会检查账本表，缺失则拒绝启动。

本地开发（stdio，DBPM 关闭）：

```yaml
server: {transport: stdio}
dbpm: {enabled: false}
databases:
  primary: {dsn: "mysql://dev@127.0.0.1:3306/app", password_env: BIZDB_DEV_PASSWORD}
institutions:
  "00002000": {scope: self}
```

## 迁移说明（v0.2 → v0.3）

- 配置从环境变量改为 `config.yaml`。`BIZDB_MYSQL_DSN`、`BIZDB_DATABASES_JSON`、`BIZDB_DBPM_*`、`BIZDB_SANDBOX_*`、`BIZDB_SESSION_ID`/`ORG_ID`/`USER_ID`/`CONVERSATION_ID` 等旧变量仍被设置时拒绝启动，并列出变量名。
- 所有工具新增必填 `instituteid`；平台清单需同步配置 `hostArguments`。
- `export` 不再上传 Sandbox，也不再返回 `dataset_id`；改为返回文本摘要 + embedded resource。
- `query` 返回 `{rows, row_count, truncated, truncated_reason, ...}`；`dry_run` 不再回显 SQL 与参数。
- DBPM 从外部桥接命令改为原生 TCP 协议；`BIZDB_DBPM_COMMAND`/`APP_NAME`/`APP_KEY`/`APP_IP` 不再使用。
- `execute` / `execute_many` 只在本地 stdio 开发可用。
