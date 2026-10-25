"""Defense-in-depth guards for single-statement reads, DML writes, and row isolation."""

from __future__ import annotations

import re
from typing import Any, Callable, Sequence

import sqlglot
import sqlparse
from sqlglot import exp
from sqlglot.dialects.mysql import MySQL
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.tokens import Token, TokenType
from sqlparse.sql import Where
from sqlparse.tokens import Comment

from .errors import INVALID_ARGUMENT, SQL_REJECTED, BizDbError
from .policy import RowFilter

# Only constructs that can actually appear *inside* an otherwise-valid single
# SELECT/DML statement are checked here. Statement-leading operations (GRANT,
# CREATE, DROP, ALTER, TRUNCATE, REVOKE, CALL, INSERT, UPDATE, DELETE,
# REPLACE) are already rejected by the leading-keyword/statement-type checks
# below, so matching them here too only produces false positives against
# identifiers that happen to share a keyword's spelling (e.g. a `grant` or
# `call` column).
_FORBIDDEN_KEYWORD_RE = re.compile(
    r"\bLOAD_FILE\b|\bINTO\s+(OUTFILE|DUMPFILE)\b",
    re.IGNORECASE,
)
_FORBIDDEN_WRITE_RE = _FORBIDDEN_KEYWORD_RE
_WRITE_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "REPLACE"}

# Row isolation rewrite (SELECT only). The limit is deliberately generous but
# finite: these statements come from a model, not a human.
MAX_SELECT_LENGTH = 100_000

# MySQL builtins that sqlglot 30.x parses as exp.Anonymous. Only these
# side-effect-free functions may survive as unknown calls; every other
# Anonymous call (including arbitrary UDFs) is rejected by default.
ALLOWED_ANONYMOUS_FUNCTIONS = frozenset(
    {
        # Date and time helpers.
        "NOW",
        "SYSDATE",
        "UNIX_TIMESTAMP",
        "ADDDATE",
        "SUBDATE",
        "ADDTIME",
        "SUBTIME",
        "TIMEDIFF",
        "MAKEDATE",
        "WEEKDAY",
        "FROM_DAYS",
        "TIME_TO_SEC",
        "SEC_TO_TIME",
        "MICROSECOND",
        "PERIOD_ADD",
        "PERIOD_DIFF",
        "GET_FORMAT",
        "TIMESTAMPADD",
        # String helpers.
        "FIND_IN_SET",
        "FIELD",
        "INSTR",
        "LPAD",
        "RPAD",
        "DATE_FORMAT",
        "STRCMP",
        "MID",
        "OCTET_LENGTH",
        "BIN",
        "OCT",
        "CONV",
        "ORD",
        # JSON, UUID, network, and pure crypto helpers.
        "JSON_UNQUOTE",
        "JSON_ARRAY",
        "JSON_LENGTH",
        "JSON_VALID",
        "JSON_CONTAINS",
        "JSON_CONTAINS_PATH",
        "JSON_DEPTH",
        "JSON_INSERT",
        "JSON_MERGE_PRESERVE",
        "JSON_PRETTY",
        "JSON_QUOTE",
        "JSON_REPLACE",
        "JSON_SEARCH",
        "JSON_STORAGE_SIZE",
        "UUID_TO_BIN",
        "BIN_TO_UUID",
        "IS_UUID",
        "INET_ATON",
        "INET_NTOA",
        "INET6_ATON",
        "INET6_NTOA",
        "IS_IPV4",
        "IS_IPV6",
        "AES_ENCRYPT",
        "AES_DECRYPT",
        # A bounded random blob is side-effect free.
        "RANDOM_BYTES",
    }
)

# Rejected by name whether sqlglot types the call or leaves it Anonymous.
_FORBIDDEN_FUNCTION_NAMES = frozenset(
    {
        "SLEEP",
        "BENCHMARK",
        "LOAD_FILE",
        "GET_LOCK",
        "RELEASE_LOCK",
        "RELEASE_ALL_LOCKS",
        "IS_FREE_LOCK",
        "IS_USED_LOCK",
        "MASTER_POS_WAIT",
        "SOURCE_POS_WAIT",
        "WAIT_FOR_EXECUTED_GTID_SET",
        "USER",
        "CURRENT_USER",
        "SESSION_USER",
        "SYSTEM_USER",
        "CONNECTION_ID",
        "LAST_INSERT_ID",
        "ROW_COUNT",
        "FOUND_ROWS",
        "VERSION",
        "CURRENT_VERSION",
        "DATABASE",
        "SCHEMA",
        "CURRENT_SCHEMA",
        "UUID_SHORT",
    }
)

# sqlglot types a few forbidden calls instead of leaving them Anonymous.
_FORBIDDEN_FUNCTION_TYPES = (
    exp.CurrentUser,
    exp.SessionUser,
    exp.CurrentVersion,
    exp.CurrentSchema,
)

_SYSTEM_SCHEMAS = frozenset({"information_schema", "mysql", "performance_schema", "sys"})

_FORBIDDEN_NODE_TYPES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.Set,
    exp.Show,
    exp.Command,
    exp.Values,
    exp.Merge,
    exp.TruncateTable,
    exp.LoadData,
    exp.Copy,
    exp.Grant,
    exp.Revoke,
    exp.Use,
    exp.Pragma,
    exp.Transaction,
    exp.Commit,
    exp.Rollback,
    exp.Analyze,
    exp.Describe,
    exp.Export,
    exp.Kill,
    exp.Unnest,
    exp.JSONTable,
    exp.Block,
    exp.Semicolon,
)

_SET_OPERATION_TYPES = (exp.Union, exp.Intersect, exp.Except)
_SELECT_ROOT_TYPES = (exp.Select, exp.Union, exp.Intersect, exp.Except)
_ALLOWED_TABLE_ARGS = frozenset({"this", "db", "catalog", "alias"})
_PARAMETER_PREFIX = "bizdb_param_"
_FILTER_PREFIX = "bizdb_filter_"


def _normalized_sql(statement) -> str:
    """Flatten *statement* back to text with comments replaced by whitespace.

    This closes obfuscation tricks like ``INTO/**/OUTFILE`` where a comment
    stands in for whitespace: regex matching against the raw SQL text would
    otherwise miss the forbidden phrase because no literal whitespace
    separates the two words.
    """
    return "".join(
        " " if tok.ttype in Comment else tok.value for tok in statement.flatten()
    )


class SqlGuardError(ValueError):
    """Raised when a SQL string fails the read-only SELECT check."""


def _single_statement(sql: str):
    if not isinstance(sql, str) or not sql.strip():
        raise SqlGuardError("empty SQL statement")
    statements = [
        s for s in sqlparse.parse(sql) if s.token_first(skip_cm=True) is not None
    ]
    if len(statements) != 1:
        raise SqlGuardError("exactly one SQL statement is allowed (no batching)")
    return statements[0]


def _leading_keyword(statement) -> str:
    first_token = statement.token_first(skip_cm=True)
    return (first_token.value or "").strip().upper()


def _has_top_level_where(statement) -> bool:
    return any(isinstance(token, Where) for token in statement.tokens)


def assert_select_only(sql: str) -> None:
    """Raise SqlGuardError unless *sql* is exactly one read-only SELECT."""
    statement = _single_statement(sql)
    stmt_type = statement.get_type()
    if stmt_type not in ("SELECT", "UNKNOWN"):
        # sqlparse classifies a leading `WITH ... SELECT` as UNKNOWN.
        raise SqlGuardError(f"only SELECT statements are allowed (got {stmt_type})")

    leading_keyword = _leading_keyword(statement)
    if leading_keyword not in ("SELECT", "WITH"):
        raise SqlGuardError(
            f"statement must start with SELECT or WITH (got {leading_keyword!r})"
        )

    if _FORBIDDEN_KEYWORD_RE.search(_normalized_sql(statement)):
        raise SqlGuardError("statement contains a forbidden keyword")


def assert_write_only(sql: str, *, require_where: bool = True) -> None:
    """Allow one bounded DML statement and reject schema/admin operations."""
    statement = _single_statement(sql)
    leading_keyword = _leading_keyword(statement)
    if leading_keyword not in _WRITE_KEYWORDS:
        raise SqlGuardError(
            "only INSERT, UPDATE, DELETE, or REPLACE statements are allowed "
            f"(got {leading_keyword!r})"
        )
    if _FORBIDDEN_WRITE_RE.search(_normalized_sql(statement)):
        raise SqlGuardError("statement contains a forbidden write operation")
    if require_where and leading_keyword in {"UPDATE", "DELETE"}:
        if not _has_top_level_where(statement):
            raise SqlGuardError(f"{leading_keyword} requires a WHERE clause")


# ---------------------------------------------------------------------------
# Row-isolation rewriting for model-authored SELECT statements
# ---------------------------------------------------------------------------


def _rejected(reason: str) -> BizDbError:
    """A redacted SQL rejection: never include SQL text or parameter values."""
    return BizDbError(SQL_REJECTED, reason)


def _coerce_params(params: Sequence[Any] | None) -> tuple[Any, ...]:
    if params is None:
        return ()
    if isinstance(params, (str, bytes)):
        raise BizDbError(INVALID_ARGUMENT, "parameters must be a sequence")
    try:
        return tuple(params)
    except TypeError as exc:
        raise BizDbError(INVALID_ARGUMENT, "parameters must be a sequence") from exc


def _tokenize(sql: str) -> list[Token]:
    try:
        return MySQL.Tokenizer().tokenize(sql)
    except Exception as exc:  # sqlglot raises several tokenizer error types
        raise _rejected("SQL could not be tokenized") from exc


def _skip_quoted(sql: str, start: int) -> int:
    """Return the index just past the quoted string or identifier at *start*."""
    quote = sql[start]
    index = start + 1
    length = len(sql)
    while index < length:
        char = sql[index]
        if char == "\\" and quote != "`":
            # MySQL backslash escapes inside strings. sqlglot applies the same
            # rule when it tokenizes.
            index += 2
            continue
        if char == quote:
            if index + 1 < length and sql[index + 1] == quote:
                index += 2
                continue
            return index + 1
        index += 1
    return length


def _has_comment_marker(sql: str) -> bool:
    """True when a MySQL comment marker appears outside a string/identifier."""
    index = 0
    length = len(sql)
    while index < length:
        char = sql[index]
        if char in ("'", '"', "`"):
            index = _skip_quoted(sql, index)
            continue
        if char == "#" or sql.startswith("--", index) or sql.startswith("/*", index):
            return True
        index += 1
    return False


def _reject_comments(sql: str, tokens: list[Token]) -> None:
    # sqlglot attaches comments to the token that follows them; MySQL executes
    # version comments (/*! ... */) and honours optimizer hints (/*+ ... */),
    # so every comment is refused. The raw scan also catches a comment that
    # the tokenizer did not attach to any token.
    if any(token.comments for token in tokens) or _has_comment_marker(sql):
        raise _rejected("comments are not allowed")


def _placeholder_spans(tokens: list[Token]) -> list[tuple[int, int]]:
    """Token spans of pymysql ``%s`` placeholders (outside quoted tokens)."""
    spans: list[tuple[int, int]] = []
    for index, token in enumerate(tokens[:-1]):
        if token.token_type is not TokenType.MOD:
            continue
        following = tokens[index + 1]
        if following.text == "s" and following.start == token.end + 1:
            spans.append((token.start, following.end))
    return spans


def _convert_placeholders(sql: str, spans: list[tuple[int, int]]) -> tuple[str, int]:
    """Replace ``%s`` with distinct named placeholders sqlglot can parse."""
    if not spans:
        return sql, 0
    pieces: list[str] = []
    last = 0
    for index, (start, end) in enumerate(spans):
        pieces.append(sql[last:start])
        pieces.append(f":{_PARAMETER_PREFIX}{index}")
        last = end + 1
    pieces.append(sql[last:])
    return "".join(pieces), len(spans)


def _parse_statement(sql: str) -> exp.Expression:
    try:
        statements = sqlglot.parse(sql, read="mysql")
    except Exception as exc:  # ParseError and dialect fallbacks
        raise _rejected("SQL could not be parsed") from exc
    statements = [statement for statement in statements if statement is not None]
    if len(statements) != 1:
        raise _rejected("exactly one SQL statement is allowed")
    return statements[0]


def _is_select_query(node: exp.Expression | None) -> bool:
    if isinstance(node, exp.Subquery):
        # Parentheses around a query expression: (SELECT ...) UNION ...
        return _is_select_query(node.this)
    if isinstance(node, exp.Select):
        return True
    if isinstance(node, _SET_OPERATION_TYPES):
        return _is_select_query(node.this) and _is_select_query(node.expression)
    return False


def _function_name(function: exp.Func) -> str:
    if isinstance(function, exp.Anonymous):
        return str(function.name).upper()
    return str(function.sql_name()).upper()


def _validate_shape(expression: exp.Expression) -> None:
    """Reject every statement shape that is not a plain read."""
    if not isinstance(expression, _SELECT_ROOT_TYPES) or not _is_select_query(
        expression
    ):
        raise _rejected("only SELECT queries are allowed")
    for cte in expression.find_all(exp.CTE):
        # A CTE body must be a query expression; sqlglot accepts some
        # non-select bodies (for example ``TABLE t``) that MySQL would reject.
        if not _is_select_query(cte.this):
            raise _rejected("only SELECT queries are allowed")

    for node in expression.walk():
        if isinstance(node, exp.Lock):
            raise _rejected("locking reads are not allowed")
        if isinstance(node, exp.Into):
            raise _rejected("SELECT INTO is not allowed")
        if isinstance(node, (exp.Parameter, exp.SessionParameter, exp.PropertyEQ)):
            raise _rejected("user and system variables are not allowed")
        if isinstance(node, (exp.JSONTable, exp.Unnest)):
            raise _rejected("table functions are not supported")
        if isinstance(node, _FORBIDDEN_NODE_TYPES):
            raise _rejected("statement contains an unsupported operation")
        if isinstance(node, exp.Column) and node.args.get("db"):
            # Aliasing a rewritten table would silently change the meaning of
            # schema.table.column, so fail closed instead.
            raise _rejected("three-part column references are not supported")

    for join in expression.find_all(exp.Join):
        if join.args.get("using"):
            raise _rejected("NATURAL and USING joins are not supported")
        if str(join.args.get("method") or "").upper() == "NATURAL":
            raise _rejected("NATURAL and USING joins are not supported")

    for function in expression.find_all(exp.Func):
        name = _function_name(function)
        if name in _FORBIDDEN_FUNCTION_NAMES or isinstance(
            function, _FORBIDDEN_FUNCTION_TYPES
        ):
            raise _rejected("function is not allowed")
        if isinstance(function, exp.Anonymous) and name not in ALLOWED_ANONYMOUS_FUNCTIONS:
            raise _rejected("function is not allowed")


def _base_tables(expression: exp.Expression) -> list[exp.Table]:
    """Return real base-table sources in document order, failing closed."""
    try:
        scopes = list(traverse_scope(expression))
    except Exception as exc:
        raise _rejected("unsupported query shape") from exc

    base_ids: set[int] = set()
    cte_ids: set[int] = set()
    for scope in scopes:
        for table in scope.tables:
            source = scope.sources.get(table.alias_or_name)
            if isinstance(source, exp.Table):
                base_ids.add(id(table))
            elif isinstance(source, Scope):
                cte_ids.add(id(table))
            else:
                # A source sqlglot cannot resolve must never be treated as an
                # unfiltered table.
                raise _rejected("unresolved table reference")

    ordered: list[exp.Table] = []
    for table in expression.find_all(exp.Table):
        node_id = id(table)
        if node_id in base_ids and node_id in cte_ids:
            raise _rejected("unresolved table reference")
        if node_id in base_ids:
            ordered.append(table)
        elif node_id not in cte_ids:
            raise _rejected("unresolved table reference")
    return ordered


def _rewrite_table(
    table: exp.Table,
    *,
    default_schema: str,
    table_filter: Callable[[str, str], RowFilter | None],
    values: dict[str, Any],
    filter_index: int,
) -> int:
    """Filter or schema-qualify one base table; return the next filter index."""
    if not isinstance(table.this, exp.Identifier):
        raise _rejected("table functions are not supported")
    if set(table.args) - _ALLOWED_TABLE_ARGS:
        raise _rejected("unsupported table modifier")
    if table.args.get("catalog") is not None:
        raise _rejected("three-part table names are not supported")
    name = table.name
    if not name:
        raise _rejected("table functions are not supported")
    schema = table.db or default_schema
    if schema.lower() in _SYSTEM_SCHEMAS:
        # Metadata schemas are never readable, whatever the config says.
        raise _rejected("system schemas are not accessible")

    row_filter = table_filter(schema, name)
    if row_filter is None:
        table.set("db", exp.to_identifier(schema))
        table.set("catalog", None)
        return filter_index
    if not isinstance(row_filter, RowFilter) or not row_filter.values:
        # AccessPolicy never returns an empty filter; anything else is a
        # caller bug and must not degrade into "read everything".
        raise _rejected("invalid row filter")

    alias = table.alias or name
    placeholder_names: list[str] = []
    for value in row_filter.values:
        placeholder = f"{_FILTER_PREFIX}{filter_index}"
        filter_index += 1
        placeholder_names.append(placeholder)
        values[placeholder] = value

    inner = exp.Select(
        expressions=[exp.Star()],
        from_=exp.From(
            this=exp.Table(
                this=exp.to_identifier(name),
                db=exp.to_identifier(schema),
            )
        ),
        where=exp.Where(
            this=exp.In(
                this=exp.column(row_filter.column),
                expressions=[
                    exp.Placeholder(this=placeholder)
                    for placeholder in placeholder_names
                ],
            )
        ),
    )
    table.replace(
        exp.Subquery(
            this=inner,
            alias=exp.TableAlias(this=exp.to_identifier(alias)),
        )
    )
    return filter_index


def _escape_percent(text: str) -> str:
    # pymysql always runs ``sql % args`` when args is passed, even an empty
    # tuple, so every literal percent (including those in strings and
    # identifiers) must be doubled.
    return text.replace("%", "%%")


def _render(expression: exp.Expression, values: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
    """Generate MySQL SQL and splice named placeholders back to ``%s``."""
    try:
        generated = expression.sql(dialect="mysql", identify=True)
    except Exception as exc:
        raise _rejected("SQL could not be rendered") from exc
    tokens = _tokenize(generated)

    pieces: list[str] = []
    args: list[Any] = []
    seen: set[str] = set()
    position = 0
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.start < position:
            raise _rejected("SQL could not be rendered safely")
        pieces.append(_escape_percent(generated[position:token.start]))
        if token.token_type is TokenType.COLON:
            following = tokens[index + 1] if index + 1 < len(tokens) else None
            if following is None or following.start != token.end + 1:
                raise _rejected("SQL could not be rendered safely")
            name = following.text
            if name not in values or name in seen:
                raise _rejected("SQL could not be rendered safely")
            pieces.append("%s")
            args.append(values[name])
            seen.add(name)
            position = following.end + 1
            index += 2
            continue
        pieces.append(_escape_percent(generated[token.start : token.end + 1]))
        position = token.end + 1
        index += 1
    pieces.append(_escape_percent(generated[position:]))

    if seen != set(values) or len(args) != len(values):
        raise _rejected("SQL could not be rendered safely")
    return "".join(pieces), tuple(args)


def _apply_row_limit(expression: exp.Expression, row_limit: int, offset: int) -> exp.Expression:
    """Bound the result in SQL so MySQL stops producing rows.

    A query without its own LIMIT/OFFSET gets them attached directly, which
    keeps its ORDER BY meaningful. A query that already limits itself is
    wrapped, because ORDER BY + LIMIT inside a derived table is honoured.
    """
    if isinstance(row_limit, bool) or not isinstance(row_limit, int) or row_limit < 1:
        raise BizDbError(INVALID_ARGUMENT, "row limit must be a positive integer")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise BizDbError(INVALID_ARGUMENT, "offset must be a non-negative integer")
    if expression.args.get("limit") is not None or expression.args.get("offset") is not None:
        expression = exp.Select(
            expressions=[exp.Star()],
            from_=exp.From(
                this=exp.Subquery(
                    this=expression, alias=exp.TableAlias(this=exp.to_identifier("_bizdb_q"))
                )
            ),
        )
    expression.set("limit", exp.Limit(expression=exp.Literal.number(row_limit)))
    if offset:
        expression.set("offset", exp.Offset(expression=exp.Literal.number(offset)))
    return expression


_CONTROL_CHARACTERS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def rewrite_select(
    sql: str,
    params: Sequence[Any] | None,
    *,
    default_schema: str,
    table_filter: Callable[[str, str], RowFilter | None],
    row_limit: int | None = None,
    offset: int = 0,
) -> tuple[str, tuple[Any, ...]]:
    """Rewrite one SELECT so every base table is row-filtered.

    Returns SQL with pymysql-style ``%s`` placeholders and the positional
    arguments in placeholder order. ``table_filter`` may raise
    :class:`BizDbError`; it propagates unchanged. With ``row_limit`` the
    statement is also bounded server-side (see ``_apply_row_limit``).
    """
    if not isinstance(sql, str) or not sql.strip():
        raise _rejected("SQL must be a non-empty string")
    if len(sql) > MAX_SELECT_LENGTH:
        raise _rejected("SQL is too long")
    if _CONTROL_CHARACTERS.search(sql):
        raise _rejected("control characters are not allowed")

    params_tuple = _coerce_params(params)
    # Scan before tokenizing so a comment that also makes sqlglot's tokenizer
    # fail is still reported as a comment.
    if _has_comment_marker(sql):
        raise _rejected("comments are not allowed")
    tokens = _tokenize(sql)
    _reject_comments(sql, tokens)
    if any(token.token_type is TokenType.COLON for token in tokens):
        # MySQL SELECT syntax has no use for ':' outside strings; refusing it
        # prevents a user-written named placeholder colliding with ours.
        raise _rejected("named placeholders are not supported")

    converted, placeholder_count = _convert_placeholders(sql, _placeholder_spans(tokens))
    if placeholder_count != len(params_tuple):
        raise BizDbError(
            INVALID_ARGUMENT, "placeholder count does not match parameter count"
        )

    expression = _parse_statement(converted)
    expected_parameters = {
        f"{_PARAMETER_PREFIX}{index}" for index in range(placeholder_count)
    }
    for placeholder in expression.find_all(exp.Placeholder):
        if placeholder.name not in expected_parameters:
            raise _rejected("unsupported placeholder")

    _validate_shape(expression)

    values: dict[str, Any] = {
        f"{_PARAMETER_PREFIX}{index}": value
        for index, value in enumerate(params_tuple)
    }
    filter_index = 0
    for table in _base_tables(expression):
        filter_index = _rewrite_table(
            table,
            default_schema=default_schema,
            table_filter=table_filter,
            values=values,
            filter_index=filter_index,
        )

    if row_limit is not None:
        expression = _apply_row_limit(expression, row_limit, offset)
    return _render(expression, values)
