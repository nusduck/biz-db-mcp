"""Tests for per-institution read policy decisions."""

import pytest

from biz_db_mcp import errors
from biz_db_mcp.config import TablePolicy, table_key
from biz_db_mcp.errors import BizDbError
from biz_db_mcp.institution import parse_institution
from biz_db_mcp.policy import AccessPolicy, RowFilter, TableInfo, TableRef
from conftest import make_settings


def _institution(code: str):
    # A raw platform id is a two-digit length prefix plus the normalized code.
    return parse_institution(f"0{len(code)}{code}")


def _policy(settings, code: str = "00002000") -> AccessPolicy:
    return AccessPolicy(settings, _institution(code))


def _base_table(*columns: str) -> TableInfo:
    return TableInfo(kind="BASE TABLE", columns=frozenset(c.lower() for c in columns))


def _view(*columns: str) -> TableInfo:
    return TableInfo(kind="VIEW", columns=frozenset(c.lower() for c in columns))


def _ref(table: str, schema: str = "app", database: str = "primary") -> TableRef:
    return TableRef(database=database, schema=schema, table=table)


def test_unknown_institution_is_refused():
    settings = make_settings()
    with pytest.raises(BizDbError) as excinfo:
        AccessPolicy(settings, _institution("99999999"))
    assert excinfo.value.code == errors.FORBIDDEN_INSTITUTION


def test_scope_property_exposes_configured_scope():
    policy = _policy(make_settings(), "00003000")
    assert policy.scope.scope == "institutions"
    assert policy.scope.ids == ("00003000", "00003001")
    assert policy.scope.allow_imports is False


def test_check_database_selects_default_and_named_database():
    policy = _policy(make_settings())
    assert policy.check_database(None).id == "primary"
    assert policy.check_database("primary").schema == "app"


def test_check_database_hides_unknown_database():
    policy = _policy(make_settings())
    with pytest.raises(BizDbError) as excinfo:
        policy.check_database("missing")
    assert excinfo.value.code == errors.RESOURCE_NOT_ACCESSIBLE
    assert "missing" not in excinfo.value.message


def test_global_and_per_database_read_switches_are_enforced():
    disabled_globally = _policy(make_settings({"reads": {"enabled": False}}))
    with pytest.raises(BizDbError) as excinfo:
        disabled_globally.check_database(None)
    assert excinfo.value.code == errors.READS_DISABLED

    disabled_for_db = _policy(
        make_settings({"databases": {"primary": {"allow_reads": False}}})
    )
    with pytest.raises(BizDbError) as excinfo:
        disabled_for_db.check_database("primary")
    assert excinfo.value.code == errors.READS_DISABLED


def test_missing_table_info_is_not_accessible():
    policy = _policy(make_settings())
    with pytest.raises(BizDbError) as excinfo:
        policy.table_filter(_ref("users"), None)
    assert excinfo.value.code == errors.RESOURCE_NOT_ACCESSIBLE


def test_base_table_filters_on_first_candidate_column():
    policy = _policy(make_settings())
    result = policy.table_filter(_ref("users"), _base_table("id", "ins_id_cd", "name"))
    assert result == RowFilter(column="ins_id_cd", values=("00002000",))


def test_base_table_without_a_candidate_is_never_public():
    policy = _policy(make_settings())
    with pytest.raises(BizDbError) as excinfo:
        policy.table_filter(_ref("users"), _base_table("id", "name"))
    assert excinfo.value.code == errors.RESOURCE_NOT_ACCESSIBLE


def test_table_info_columns_are_normalized_defensively():
    policy = _policy(make_settings())
    info = TableInfo(kind="BASE TABLE", columns=frozenset({"INS_ID_CD"}))
    assert policy.table_filter(_ref("users"), info) == RowFilter(
        column="ins_id_cd", values=("00002000",)
    )


def test_view_without_explicit_policy_is_denied():
    policy = _policy(make_settings())
    with pytest.raises(BizDbError) as excinfo:
        policy.table_filter(_ref("report_view"), _view("ins_id_cd", "total"))
    assert excinfo.value.code == errors.RESOURCE_NOT_ACCESSIBLE


def test_explicit_policy_allows_a_view():
    settings = make_settings(
        {
            "isolation": {
                "tables": {"primary:app.report_view": {"column": "tenant_id"}},
            }
        }
    )
    policy = _policy(settings)
    result = policy.table_filter(_ref("report_view"), _view("tenant_id", "total"))
    assert result == RowFilter(column="tenant_id", values=("00002000",))


def test_explicit_public_table_is_unfiltered_and_case_insensitive():
    settings = make_settings(
        {"isolation": {"public_tables": ["primary:app.public_table"]}}
    )
    policy = _policy(settings)
    assert policy.table_filter(_ref("PUBLIC_TABLE", schema="APP"), _view()) is None


def test_explicit_policy_with_missing_column_is_denied():
    settings = make_settings(
        {
            "isolation": {
                "tables": {"primary:app.users": {"column": "ins_id_cd"}},
            }
        }
    )
    policy = _policy(settings)
    with pytest.raises(BizDbError) as excinfo:
        policy.table_filter(_ref("users"), _base_table("id", "name"))
    assert excinfo.value.code == errors.RESOURCE_NOT_ACCESSIBLE


def test_all_scope_skips_the_row_filter_but_still_validates_the_table():
    policy = _policy(make_settings(), "00001000")
    assert policy.table_filter(_ref("users"), _base_table("ins_id_cd")) is None

    with pytest.raises(BizDbError) as excinfo:
        policy.table_filter(_ref("report_view"), _view("ins_id_cd"))
    assert excinfo.value.code == errors.RESOURCE_NOT_ACCESSIBLE

    with pytest.raises(BizDbError) as excinfo:
        policy.table_filter(_ref("users"), _base_table("id"))
    assert excinfo.value.code == errors.RESOURCE_NOT_ACCESSIBLE


def test_institutions_scope_filters_on_every_authorized_code():
    policy = _policy(make_settings(), "00003000")
    result = policy.table_filter(_ref("users"), _base_table("ins_id_cd"))
    assert result == RowFilter(
        column="ins_id_cd", values=("00003000", "00003001")
    )


def test_raw_self_scope_uses_the_raw_platform_identifier():
    settings = make_settings(
        {
            "isolation": {
                "tables": {
                    "primary:app.raw_table": {
                        "column": "org_raw",
                        "value_format": "raw",
                    }
                }
            }
        }
    )
    policy = _policy(settings, "00002000")
    result = policy.table_filter(_ref("raw_table"), _base_table("org_raw"))
    assert result == RowFilter(column="org_raw", values=("0800002000",))


def test_raw_institutions_scope_is_denied_without_a_mapping():
    settings = make_settings(
        {
            "isolation": {
                "tables": {
                    "primary:app.raw_table": {
                        "column": "org_raw",
                        "value_format": "raw",
                    }
                }
            }
        }
    )
    policy = _policy(settings, "00003000")
    with pytest.raises(BizDbError) as excinfo:
        policy.table_filter(_ref("raw_table"), _base_table("org_raw"))
    assert excinfo.value.code == errors.RESOURCE_NOT_ACCESSIBLE


def test_candidate_order_and_case_insensitive_matching_are_honoured():
    settings = make_settings(
        {
            "isolation": {
                "column_candidates": ["tenant_id", "INS_ID_CD"],
            }
        }
    )
    policy = _policy(settings)
    both = policy.table_filter(_ref("users"), _base_table("ins_id_cd", "tenant_id"))
    assert both == RowFilter(column="tenant_id", values=("00002000",))

    fallback = policy.table_filter(_ref("users"), _base_table("ins_id_cd"))
    assert fallback == RowFilter(column="INS_ID_CD", values=("00002000",))


def test_policy_keys_use_the_canonical_table_key():
    settings = make_settings(
        {"isolation": {"public_tables": ["primary:app.public_table"]}}
    )
    assert table_key("primary", "APP", "PUBLIC_TABLE") in settings.isolation.tables
    assert settings.isolation.tables[
        table_key("primary", "app", "public_table")
    ] == TablePolicy(column=None)
