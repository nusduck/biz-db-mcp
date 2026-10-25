import pytest

from biz_db_mcp.errors import (
    INVALID_ARGUMENT,
    RESOURCE_NOT_ACCESSIBLE,
    SQL_REJECTED,
    BizDbError,
    not_accessible,
)
from biz_db_mcp.policy import RowFilter
from biz_db_mcp.sql_guard import (
    MAX_SELECT_LENGTH,
    SqlGuardError,
    assert_select_only,
    assert_write_only,
    rewrite_select,
)


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


# ---------------------------------------------------------------------------
# Row-isolation rewriting
# ---------------------------------------------------------------------------

DEFAULT_FILTER = RowFilter(column="ins_id_cd", values=("I1",))


def _filter(schema: str, table: str) -> RowFilter:
    return DEFAULT_FILTER


def _rewrite(sql, params=(), table_filter=_filter):
    return rewrite_select(
        sql, params, default_schema="app", table_filter=table_filter
    )


def _format(out: str, args) -> str:
    # pymysql always applies sql % args when args is passed (even empty), so
    # formatting with a placeholder for each argument must never raise.
    return out % tuple("?" for _ in args)


def _assert_rejected(sql, params=(), table_filter=_filter):
    with pytest.raises(BizDbError) as excinfo:
        _rewrite(sql, params, table_filter=table_filter)
    assert excinfo.value.code == SQL_REJECTED
    return excinfo.value


def test_rewrite_simple_select_exact_sql_and_argument_order():
    out, args = _rewrite("SELECT id FROM users WHERE name = %s", ("alice",))

    assert out == (
        "SELECT `id` FROM (SELECT * FROM `app`.`users` "
        "WHERE `ins_id_cd` IN (%s)) AS `users` WHERE `name` = %s"
    )
    assert args == ("I1", "alice")
    assert _format(out, args) == (
        "SELECT `id` FROM (SELECT * FROM `app`.`users` "
        "WHERE `ins_id_cd` IN (?)) AS `users` WHERE `name` = ?"
    )


def test_rewrite_keeps_parameter_before_and_after_the_filter_value():
    out, args = _rewrite(
        "SELECT %s AS tag, id FROM users WHERE name = %s", ("tag", "alice")
    )

    assert out == (
        "SELECT %s AS `tag`, `id` FROM (SELECT * FROM `app`.`users` "
        "WHERE `ins_id_cd` IN (%s)) AS `users` WHERE `name` = %s"
    )
    assert args == ("tag", "I1", "alice")


def test_rewrite_join_filters_both_tables_exact_sql():
    out, args = _rewrite(
        "SELECT u.id, o.id FROM users u JOIN orders o ON o.uid = u.id"
    )

    assert out == (
        "SELECT `u`.`id`, `o`.`id` FROM (SELECT * FROM `app`.`users` "
        "WHERE `ins_id_cd` IN (%s)) AS `u` JOIN (SELECT * FROM `app`.`orders` "
        "WHERE `ins_id_cd` IN (%s)) AS `o` ON `o`.`uid` = `u`.`id`"
    )
    assert args == ("I1", "I1")


def test_rewrite_left_join_filters_inside_the_derived_table():
    out, args = _rewrite(
        "SELECT * FROM accounts a LEFT JOIN users u ON u.account_id = a.id"
    )

    assert (
        "LEFT JOIN (SELECT * FROM `app`.`users` WHERE `ins_id_cd` IN (%s)) AS `u`"
        in out
    )
    assert out.count("`ins_id_cd`") == 2
    # Only the two derived-table filters; no outer WHERE that would turn the
    # LEFT JOIN into an inner join.
    assert out.count("WHERE") == 2
    assert " ON `u`.`account_id` = `a`.`id`" in out
    assert args == ("I1", "I1")


def test_rewrite_preserves_where_or():
    out, args = _rewrite("SELECT * FROM a WHERE x = 1 OR y = 2")

    assert out == (
        "SELECT * FROM (SELECT * FROM `app`.`a` WHERE `ins_id_cd` IN (%s)) "
        "AS `a` WHERE `x` = 1 OR `y` = 2"
    )
    assert args == ("I1",)


def test_rewrite_group_by_and_having_stay_outside_the_filter():
    out, args = _rewrite(
        "SELECT x, COUNT(*) AS c FROM t GROUP BY x HAVING c > 1"
    )

    assert (
        "(SELECT * FROM `app`.`t` WHERE `ins_id_cd` IN (%s)) AS `t`" in out
    )
    assert out.endswith("GROUP BY `x` HAVING `c` > 1")
    assert args == ("I1",)


def test_rewrite_filters_subquery_in_where_in_and_exists():
    in_query, in_args = _rewrite(
        "SELECT * FROM t WHERE id IN (SELECT tid FROM u WHERE flag = 1)"
    )
    exists_query, exists_args = _rewrite(
        "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)"
    )

    assert in_query.count("`ins_id_cd`") == 2
    assert exists_query.count("`ins_id_cd`") == 2
    assert in_args == ("I1", "I1")
    assert exists_args == ("I1", "I1")


def test_rewrite_filters_subquery_in_select_list():
    out, args = _rewrite("SELECT (SELECT MAX(v) FROM u) AS m FROM t")

    assert out.count("`ins_id_cd`") == 2
    assert "AS `u`" in out and "AS `t`" in out
    assert args == ("I1", "I1")


def test_rewrite_cte_filters_the_base_table_but_not_the_cte_reference():
    out, args = _rewrite("WITH c AS (SELECT * FROM base) SELECT * FROM c")

    assert out == (
        "WITH `c` AS (SELECT * FROM (SELECT * FROM `app`.`base` "
        "WHERE `ins_id_cd` IN (%s)) AS `base`) SELECT * FROM `c`"
    )
    assert args == ("I1",)


def test_rewrite_cte_name_shadowing_a_real_table_is_not_a_base_table():
    calls: list[tuple[str, str]] = []

    def recording_filter(schema: str, table: str) -> RowFilter:
        calls.append((schema, table))
        return DEFAULT_FILTER

    out, args = _rewrite(
        "WITH users AS (SELECT * FROM users) SELECT * FROM users",
        table_filter=recording_filter,
    )

    # Only the table inside the CTE body is physical; the outer reference is
    # the CTE itself.
    assert calls == [("app", "users")]
    assert out.count("`ins_id_cd`") == 1
    assert out.endswith("SELECT * FROM `users`")
    assert args == ("I1",)


def test_rewrite_union_filters_each_branch():
    out, args = _rewrite("SELECT * FROM t1 UNION ALL SELECT * FROM t2")

    assert out.count("`ins_id_cd`") == 2
    assert "UNION ALL" in out
    assert args == ("I1", "I1")


def test_rewrite_unfiltered_table_is_schema_qualified():
    out, args = _rewrite(
        "SELECT * FROM t WHERE a = %s",
        ("x",),
        table_filter=lambda schema, table: None,
    )

    assert out == "SELECT * FROM `app`.`t` WHERE `a` = %s"
    assert args == ("x",)


def test_rewrite_keeps_an_explicit_schema_when_unfiltered():
    out, _ = _rewrite(
        "SELECT * FROM db.t",
        table_filter=lambda schema, table: None,
    )

    assert out == "SELECT * FROM `db`.`t`"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM information_schema.tables",
        "SELECT * FROM mysql.user",
        "SELECT * FROM performance_schema.events",
        "SELECT * FROM sys.x",
    ],
)
def test_rewrite_rejects_system_schemas_without_calling_the_filter(sql):
    calls: list[tuple[str, str]] = []

    def recording_filter(schema: str, table: str) -> RowFilter:
        calls.append((schema, table))
        return DEFAULT_FILTER

    _assert_rejected(sql, table_filter=recording_filter)
    assert calls == []


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM t -- trailing comment",
        "SELECT * FROM t # trailing comment",
        "SELECT * FROM t /* block comment */",
        "SELECT /*!50000 * FROM t",
        "SELECT /*+ BKA(t) */ * FROM t",
        "SELECT 1 -- no space comment",
    ],
)
def test_rewrite_rejects_every_comment_kind(sql):
    error = _assert_rejected(sql)
    assert "comment" in error.message


def test_rewrite_allows_comment_markers_inside_literals_and_identifiers():
    out, _ = _rewrite(
        "SELECT '--' AS a, '#' AS b, '/*' AS c, `d#e--f` AS d FROM t"
    )

    assert "'--'" in out
    assert "'#'" in out
    assert "'/*'" in out
    assert "`d#e--f`" in out


def test_rewrite_percent_inside_a_string_literal_is_not_a_placeholder():
    out, args = _rewrite("SELECT '%s' AS lit FROM t")

    assert args == ("I1",)
    assert "'%%s'" in out
    assert "'%s'" in _format(out, args)


def test_rewrite_like_percent_survives_pymysql_formatting():
    out, args = _rewrite("SELECT * FROM t WHERE name LIKE 'a%'")

    assert "LIKE 'a%%'" in out
    formatted = out % args
    assert "LIKE 'a%'" in formatted


def test_rewrite_escapes_a_modulo_operator():
    out, args = _rewrite("SELECT 5 % 2 FROM t")

    assert "5 %% 2" in out
    assert "5 % 2" in _format(out, args)


def test_rewrite_escapes_percent_inside_identifiers():
    out, args = _rewrite("SELECT `a%b` FROM t")

    assert "`a%%b`" in out
    assert "`a%b`" in _format(out, args)


def test_rewrite_uses_one_placeholder_per_filter_value():
    two_values = RowFilter(column="ins_id_cd", values=("I1", "I2"))
    out, args = _rewrite(
        "SELECT * FROM t",
        table_filter=lambda schema, table: two_values,
    )

    assert "IN (%s, %s)" in out
    assert args == ("I1", "I2")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM t FOR UPDATE",
        "SELECT * FROM t FOR SHARE",
        "SELECT * FROM t LOCK IN SHARE MODE",
        "SELECT * FROM t INTO OUTFILE '/tmp/leak'",
        "SELECT 1 INTO @v",
        "SELECT * FROM t WHERE id = @x",
        "SELECT @@version",
        "SELECT @x := 1",
        "SELECT SLEEP(1)",
        "SELECT BENCHMARK(1, MD5('x'))",
        "SELECT LOAD_FILE('/etc/passwd')",
        "SELECT GET_LOCK('x', 1)",
        "SELECT USER()",
        "SELECT CURRENT_USER()",
        "SELECT VERSION()",
        "SELECT DATABASE()",
        "SELECT CONNECTION_ID()",
        "SELECT foo(1)",
        "SELECT * FROM t1; DROP TABLE t2",
        "DELETE FROM t",
        "UPDATE t SET x = 1",
        "INSERT INTO t VALUES (1)",
        "SET @x = 1",
        "SHOW TABLES",
        "CREATE TABLE x (id INT)",
        "SELECT * FROM a.b.c",
        "SELECT db.t.col FROM db.t",
        "SELECT * FROM a NATURAL JOIN b",
        "SELECT * FROM a JOIN b USING (id)",
        "SELECT * FROM (VALUES ROW(1)) AS v",
        "SELECT * FROM t USE INDEX (i)",
        "SELECT * FROM t PARTITION (p0)",
        "SELECT * FROM JSON_TABLE(x, '$' COLUMNS(a INT)) AS jt",
        "SELECT * FROM my_table_function(1) AS x",
    ],
)
def test_rewrite_rejects_unsupported_statements(sql):
    _assert_rejected(sql)


def test_rewrite_allows_known_side_effect_free_anonymous_functions():
    out, args = _rewrite(
        "SELECT FIND_IN_SET('a', 'a,b'), NOW(), MICROSECOND(NOW()) FROM t"
    )

    assert "FIND_IN_SET" in out
    assert "NOW()" in out
    assert "MICROSECOND" in out
    assert args == ("I1",)


def test_rewrite_rejects_a_cte_body_that_is_not_a_select():
    _assert_rejected("WITH c AS (TABLE t) SELECT * FROM c")
    _assert_rejected("WITH c AS (t) SELECT * FROM c")


def test_rewrite_rejects_placeholder_count_mismatches():
    with pytest.raises(BizDbError) as excinfo:
        _rewrite("SELECT * FROM t WHERE id = %s")
    assert excinfo.value.code == INVALID_ARGUMENT

    with pytest.raises(BizDbError) as excinfo:
        _rewrite("SELECT * FROM t", ("unused",))
    assert excinfo.value.code == INVALID_ARGUMENT

    # A '%s' inside a string literal is not a placeholder and must not count.
    with pytest.raises(BizDbError) as excinfo:
        _rewrite("SELECT '%s' FROM t", ("unused",))
    assert excinfo.value.code == INVALID_ARGUMENT


def test_rewrite_rejects_other_placeholder_syntax():
    _assert_rejected("SELECT :named FROM t")
    _assert_rejected("SELECT ? FROM t")


def test_rewrite_rejects_empty_and_oversized_sql():
    _assert_rejected("")
    _assert_rejected("   ")
    _assert_rejected("SELECT 1" + " " * MAX_SELECT_LENGTH)


def test_rewrite_propagates_policy_denials_unchanged():
    def deny(schema: str, table: str):
        raise not_accessible("table")

    with pytest.raises(BizDbError) as excinfo:
        _rewrite("SELECT * FROM t", table_filter=deny)
    assert excinfo.value.code == RESOURCE_NOT_ACCESSIBLE


def test_rewrite_errors_never_echo_sql_or_parameters():
    secret = "super-secret-value"
    with pytest.raises(BizDbError) as excinfo:
        _rewrite("SELECT * FROM t WHERE x = %s FOR UPDATE", (secret,))

    message = excinfo.value.message
    assert "SELECT" not in message
    assert secret not in message
    assert message == "locking reads are not allowed"


def test_rewrite_with_no_tables_keeps_the_statement():
    out, args = _rewrite("SELECT 1")

    assert out == "SELECT 1"
    assert args == ()
    assert _format(out, args) == "SELECT 1"


# ---- server-side row bound (added during integration) ----

def _filter_a(schema, table):
    from biz_db_mcp.policy import RowFilter

    return RowFilter("ins_id_cd", ("A",))


def test_row_limit_is_attached_without_breaking_order_by():
    from biz_db_mcp.sql_guard import rewrite_select

    out, args = rewrite_select(
        "SELECT a FROM t ORDER BY a", None, default_schema="app",
        table_filter=_filter_a, row_limit=11, offset=5,
    )
    assert out.endswith("ORDER BY `a` LIMIT 11 OFFSET 5")
    assert args == ("A",)


def test_query_with_its_own_limit_is_wrapped():
    from biz_db_mcp.sql_guard import rewrite_select

    out, args = rewrite_select(
        "SELECT a FROM t ORDER BY a LIMIT %s", [3], default_schema="app",
        table_filter=_filter_a, row_limit=201,
    )
    assert out.startswith("SELECT * FROM (SELECT `a` FROM")
    assert out.endswith("ORDER BY `a` LIMIT %s) AS `_bizdb_q` LIMIT 201")
    assert args == ("A", 3)


def test_control_characters_are_rejected():
    from biz_db_mcp.errors import BizDbError
    from biz_db_mcp.sql_guard import rewrite_select

    with pytest.raises(BizDbError, match="control characters"):
        rewrite_select("SELECT * FROM t\x00", None, default_schema="app", table_filter=_filter_a)
    rewrite_select("SELECT *\n\tFROM t", None, default_schema="app", table_filter=_filter_a)
