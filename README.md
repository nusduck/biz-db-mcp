# biz-db-mcp

一个面向 Agent 的 MySQL MCP Server，支持多数据库、只读查询、受保护的 DML 写入，以及批量导出到 Sandbox Dataset。

## 设计边界

- 默认只读：`BIZDB_WRITE_ENABLED` 默认关闭，单个数据库还必须显式 `allow_writes: true`。
- `query` 与 `export` 只允许单条 `SELECT`/`WITH ... SELECT`；`execute` 只允许单条 `INSERT`、`UPDATE`、`DELETE` 或 `REPLACE`。
- `UPDATE`/`DELETE` 默认必须带 `WHERE`，写入在事务中执行，并受最大影响行数限制。
- `export` 使用服务端游标分批写 Parquet，再上传 Sandbox；身份从进程环境注入，模型不能指定目标 session。
- SQL 审计只记录 SHA-256 和长度，不记录原始 SQL、参数或密码。

## 目录结构

```text
biz_db_mcp/
  config.py          # 环境变量、多数据库、写入和 DBPM 配置
  db.py              # MySQL 连接、查询、写入
  dbpm.py            # DBPM 外部桥接命令适配器
  sql_guard.py       # SELECT/DML 语句边界
  export.py          # 流式 Parquet 导出
  sandbox_client.py  # Dataset 上传
  audit.py           # 结构化审计日志
  server.py          # MCP 工具注册
tests/               # 单元测试
```

## 配置

单库兼容配置：`BIZDB_MYSQL_DSN=mysql://user:password@host:3306/database`。

多库使用 `BIZDB_DATABASES_JSON`，格式可以是映射或 `connections` 数组：

```json
{"connections":[
  {"id":"primary","dsn":"mysql://user:password@db-a/app"},
  {"id":"reporting","dsn":"mysql://user:password@db-b/reporting","allow_writes":false}
]}
```

`BIZDB_DEFAULT_DATABASE` 选择默认库。写入还需要 `BIZDB_WRITE_ENABLED=true`，并在目标库配置 `allow_writes: true`；可用 `BIZDB_WRITE_MAX_AFFECTED_ROWS` 和 `BIZDB_WRITE_REQUIRE_WHERE` 调整保护阈值。

工具包括 `list_databases`、`describe_table`、`query`、`execute` 和 `export`。`execute` 是单语句、事务提交的 DML 接口，不提供 DDL、事务控制或任意多语句执行。

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
