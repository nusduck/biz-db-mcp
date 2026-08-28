# biz-db-mcp

一个面向 Agent 的 MySQL MCP Server，支持多数据库、只读查询、受保护的 DML 写入，以及批量导出到 Sandbox Dataset。

> v0.2 灵活性升级：按库覆盖配置、WHERE 白名单、`list_tables`/`explain` 勘探、`dry_run` 预检、`execute_many` 批量事务、`offset` 分页与 `csv/jsonl` 导出。

## 设计边界

- 默认只读：`BIZDB_WRITE_ENABLED` 默认关闭，单个数据库还必须显式 `allow_writes: true`。
- `query` 与 `export` 只允许单条 `SELECT`/`WITH ... SELECT`；`execute` 只允许单条 `INSERT`、`UPDATE`、`DELETE` 或 `REPLACE`，`execute_many` 允许单事务批量（最多 100 条）。
- `UPDATE`/`DELETE` 默认必须带 `WHERE`，可通过 `BIZDB_WRITE_REQUIRE_WHERE_EXCEPT_TABLES` 按表名 glob 放宽（如 `temp_*,cache_*`）；写入在事务中执行，并受最大影响行数限制（支持按库覆盖）。
- `export` 使用服务端游标分批写 Parquet/CSV/JSONL，再上传 Sandbox；身份从进程环境注入，模型不能指定目标 session。
- SQL 审计只记录 SHA-256 和长度，不记录原始 SQL、参数或密码。
- 所有限流与超时均支持按库覆盖，全局配置为 fallback。

## 目录结构

```text
biz_db_mcp/
  config.py          # 环境变量、多数据库、写入和 DBPM 配置（含按库覆盖与文件加载）
  db.py              # MySQL 连接、查询、写入、勘探（describe/list_tables/explain/批量）
  dbpm.py            # DBPM 外部桥接命令适配器
  sql_guard.py       # SELECT/DML 语句边界
  export.py          # 流式 Parquet/CSV/JSONL 导出
  sandbox_client.py  # Dataset 上传（多格式 MIME）
  audit.py           # 结构化审计日志
  server.py          # MCP 工具注册（8 个工具）
tests/               # 单元测试
```

## 配置

单库兼容配置：`BIZDB_MYSQL_DSN=mysql://user:password@host:3306/database`。

多库使用 `BIZDB_DATABASES_JSON`，格式可以是映射或 `connections` 数组：

```json
{"connections":[
  {"id":"primary","dsn":"mysql://user:password@db-a/app","query_max_rows":200},
  {"id":"reporting","dsn":"mysql://user:password@db-b/reporting","allow_writes":false,"query_max_rows":5000}
]}
```

也支持文件加载：`BIZDB_DATABASES_FILE=/etc/biz-db.json`（内容与 `BIZDB_DATABASES_JSON` 同格式，优先级：`BIZDB_DATABASES_JSON` > `BIZDB_DATABASES_FILE` > `BIZDB_MYSQL_DSN`）。

`BIZDB_DEFAULT_DATABASE` 选择默认库。写入还需要 `BIZDB_WRITE_ENABLED=true`，并在目标库配置 `allow_writes: true`；可用 `BIZDB_WRITE_MAX_AFFECTED_ROWS` 和 `BIZDB_WRITE_REQUIRE_WHERE` 调整保护阈值。按库覆盖示例：

```json
{"id":"writer","dsn":"mysql://...","allow_writes":true,"write_max_affected_rows":5000,"write_require_where":false,"query_timeout_seconds":30}
```

支持的按库覆盖字段：`query_max_rows`、`query_timeout_seconds`、`export_max_rows`、`export_timeout_seconds`、`write_max_affected_rows`、`write_require_where`（`null` 表示继承全局）。

WHERE 白名单：`BIZDB_WRITE_REQUIRE_WHERE_EXCEPT_TABLES=temp_*,cache_logs`（逗号或空格分隔，glob 匹配，大小写不敏感），命中白名单的表允许 `UPDATE/DELETE` 不带 `WHERE`。

### 工具一览（8 个）

| 工具 | 说明 | 灵活性增强 |
|---|---|---|
| `list_databases` | 列出已配置库，不暴露 DSN | 按库覆盖后仍脱敏返回 |
| `describe_table` | 返回表结构，支持 `schema.table` | 支持 `db.table` 限定 |
| `list_tables` | 列出库内表/视图，支持 `pattern`（LIKE）过滤 | 新增 |
| `query` | 受控只读 SELECT，`limit` 受按库上限约束 | 新增 `offset` 分页、`dry_run` 预检 |
| `explain` | 返回 `EXPLAIN` 执行计划，不实际执行 | 新增 |
| `execute` | 单条 DML 事务提交 | 新增 `dry_run`（EXPLAIN/ROLLBACK 预估） |
| `execute_many` | 多条 DML 单事务批量（≤100 条） | 新增，`atomic` 控制是否全回滚 |
| `export` | 流式导出到 Sandbox Dataset | 新增 `format: parquet/csv/jsonl` |

`execute` 仍为单语句 DML 接口，不提供 DDL、事务控制或多语句拼接；批量场景请用 `execute_many`。

## DBPM（可选）

UPspec 中的 DBPM 是密码管理系统，不是 MCP 请求认证。Python 项目不内置私有 Java SDK；启用 `BIZDB_DBPM_ENABLED=true` 后，必须配置 `BIZDB_DBPM_COMMAND`、`BIZDB_DBPM_SERVERS`、`BIZDB_DBPM_APP_NAME`、`BIZDB_DBPM_APP_KEY` 和 `BIZDB_DBPM_APP_IP`。

桥接命令从 stdin 读取 JSON（包含 database、username、servers 等字段），只在 stdout 输出密码，非零退出码表示失败。DBPM 私钥通过 `DBPM_APP_KEY` 环境变量传给桥接进程，不出现在命令行参数中。数据库条目可用 `dbpm_database`/`dbpm_user` 覆盖 DBPM 查询键。

## 开发与运行

```bash
python3 -m pip install -e '.[dev]'
python3 -m pytest
biz-db-mcp
```

Sandbox 上传仍需配置 `BIZDB_SANDBOX_BASE_URL`、`BIZDB_SANDBOX_API_TOKEN`；导出身份需配置 `BIZDB_SESSION_ID`、`BIZDB_ORG_ID`、`BIZDB_USER_ID`。生产环境应使用 DBPM，不要把真实密码提交到配置、日志或测试夹具中。

### 灵活性使用示例

```python
# 分页查询
query("SELECT id, name FROM users WHERE active=1", limit=50, offset=100)

# 预检（不查库）
query("SELECT * FROM orders WHERE status=%s", ["paid"], dry_run=True)
# -> {"dry_run": True, "effective_limit": 200, "effective_timeout": 10}

explain("SELECT * FROM orders WHERE status=%s", ["paid"])
list_tables(pattern="order%", include_views=False)
describe_table("reporting.orders")

# 写入预估

execute("UPDATE users SET active=0 WHERE id=%s", [123], dry_run=True)
# -> {"dry_run": True, "estimated_plan": [...] }

# 批量事务
execute_many([
  {"sql": "INSERT INTO users(name) VALUES (%s)", "params": ["alice"]},
  {"sql": "UPDATE users SET active=1 WHERE id=%s", "params": [123]}
], atomic=True)

# 多格式导出
export("SELECT * FROM orders", dataset_name="orders_2024", format="csv")
export("SELECT * FROM orders", dataset_name="orders_2024", format="jsonl")
```

### 环境变量速查（新增标记 ★）

| 变量 | 说明 | 默认 |
|---|---|---|
| `BIZDB_DATABASES_FILE` ★ | 多库 JSON 文件路径 | - |
| `BIZDB_WRITE_REQUIRE_WHERE_EXCEPT_TABLES` ★ | 免 WHERE 白名单（glob） | - |
| 单库按库覆盖 ★ | `query_max_rows` 等 6 个字段可在 `BIZDB_DATABASES_JSON` 单库内覆盖 | 继承全局 |
