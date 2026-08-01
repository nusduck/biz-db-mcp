import json

from biz_db_mcp.audit import log_call


def test_audit_does_not_emit_raw_sql(capsys):
    secret_sql = "SELECT password FROM users WHERE id = 1"

    with log_call("query", sql=secret_sql):
        pass

    captured = capsys.readouterr().err
    record = json.loads(captured)
    assert secret_sql not in captured
    assert record["sql_length"] == len(secret_sql)
    assert record["sql_sha256"]
