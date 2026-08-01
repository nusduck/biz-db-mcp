import pytest

from biz_db_mcp.sql_guard import SqlGuardError, assert_select_only, assert_write_only


def test_select_and_cte_are_allowed():
    assert_select_only("SELECT id FROM users")
    assert_select_only("WITH recent AS (SELECT id FROM users) SELECT * FROM recent")


def test_select_batch_is_rejected():
    with pytest.raises(SqlGuardError):
        assert_select_only("SELECT 1; DELETE FROM users")


def test_select_blocks_into_outfile_even_with_comment_obfuscation():
    with pytest.raises(SqlGuardError):
        assert_select_only("SELECT * FROM users INTO OUTFILE '/tmp/x'")
    with pytest.raises(SqlGuardError):
        assert_select_only("SELECT * FROM users INTO/**/OUTFILE '/tmp/x'")
    with pytest.raises(SqlGuardError):
        assert_select_only("SELECT LOAD_FILE('/etc/passwd')")


def test_select_allows_columns_named_like_forbidden_keywords():
    assert_select_only("SELECT grant FROM budgets WHERE id = 1")
    assert_select_only("SELECT id, call FROM logs")


def test_write_requires_explicit_dml_shape():
    assert_write_only("UPDATE users SET active = %s WHERE id = %s")
    assert_write_only("INSERT INTO users(name) VALUES (%s)")

    with pytest.raises(SqlGuardError, match="WHERE"):
        assert_write_only("DELETE FROM users")
    with pytest.raises(SqlGuardError, match="WHERE"):
        assert_write_only(
            "UPDATE users SET value = (SELECT max(value) FROM other WHERE id = 1)"
        )
    with pytest.raises(SqlGuardError):
        assert_write_only("DROP TABLE users")
