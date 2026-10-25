"""Row-isolation policy for model-authored reads.

Many institutions share one MySQL database. The caller's institution is
injected by the trusted platform, never chosen by the model, and every table
read on its behalf is filtered down to the rows it may see. This module turns
configuration plus trusted metadata into a :class:`RowFilter`; it never sees
SQL. ``sql_guard.rewrite_select`` applies the resulting filters.

All denials use :func:`biz_db_mcp.errors.not_accessible` so that a
cross-institution denial is indistinguishable from a table that does not
exist.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import errors
from .config import (
    ConfigError,
    DatabaseConfig,
    InstitutionScope,
    Settings,
    table_key,
)
from .errors import FORBIDDEN_INSTITUTION, READS_DISABLED, BizDbError
from .institution import Institution


@dataclass(frozen=True)
class TableRef:
    """One physical table as named by a query."""

    database: str
    schema: str
    table: str


@dataclass(frozen=True)
class TableInfo:
    """Trusted metadata for one table: its kind and lower-case columns."""

    kind: str  # "BASE TABLE" | "VIEW"
    columns: frozenset[str]


@dataclass(frozen=True)
class RowFilter:
    """An equality/in filter to apply to one column; values are never empty."""

    column: str
    values: tuple[str, ...]


class AccessPolicy:
    """Per-call read authorization for one trusted institution."""

    def __init__(self, settings: Settings, institution: Institution) -> None:
        if institution.code not in settings.institutions:
            # Unknown institutions are refused outright; never fall back to a
            # wider default scope.
            raise BizDbError(FORBIDDEN_INSTITUTION, "institution is not authorized")
        self._settings = settings
        self._institution = institution
        self._scope = settings.institutions[institution.code]

    @property
    def scope(self) -> InstitutionScope:
        return self._scope

    def check_database(self, database_id: str | None) -> DatabaseConfig:
        """Return the selected database config or refuse the read."""
        try:
            database = self._settings.database(database_id)
        except ConfigError as exc:
            raise errors.not_accessible("database") from exc
        if not self._settings.reads_enabled or not database.allow_reads:
            raise BizDbError(READS_DISABLED, "reads are disabled for this database")
        return database

    def table_filter(self, ref: TableRef, info: TableInfo | None) -> RowFilter | None:
        """Return the row filter for *ref*, ``None`` for a public table, or deny."""
        if info is None:
            # Missing and invisible tables share one response.
            raise errors.not_accessible("table")
        # TableInfo documents lower-case columns; normalize defensively so a
        # caller that forgot to lower-case cannot turn into a wider read.
        columns = frozenset(column.lower() for column in info.columns)

        policy = self._settings.isolation.tables.get(
            table_key(ref.database, ref.schema, ref.table)
        )
        if policy is not None:
            if policy.public:
                return None
            column = policy.column
            if column is None:  # defensive: public was handled above
                raise errors.not_accessible("table")
            if column.lower() not in columns:
                # The configured isolation column is not part of the table:
                # deny instead of silently reading every row.
                raise errors.not_accessible("table")
            value_format = policy.value_format
        else:
            if info.kind != "BASE TABLE":
                # Views must be opted in explicitly; their definition may not
                # expose the isolation column at all.
                raise errors.not_accessible("table")
            column = self._candidate_column(columns)
            value_format = "code"

        return self._row_filter(column, value_format)

    def _candidate_column(self, columns: frozenset[str]) -> str:
        for candidate in self._settings.isolation.column_candidates:
            if candidate.lower() in columns:
                return candidate
        # A base table without any known institution column is never public.
        raise errors.not_accessible("table")

    def _row_filter(self, column: str, value_format: str) -> RowFilter | None:
        scope = self._scope
        if scope.scope == "all":
            # "all" only widens rows after the table itself passed every check.
            return None
        if scope.scope == "self":
            value = (
                self._institution.code
                if value_format == "code"
                else self._institution.raw
            )
            return RowFilter(column=column, values=(value,))
        if scope.scope == "institutions":
            if value_format == "raw":
                # No verified raw-id mapping exists for a peer institution.
                raise errors.not_accessible("table")
            if not scope.ids:
                raise errors.not_accessible("table")
            return RowFilter(column=column, values=scope.ids)
        raise errors.not_accessible("table")
