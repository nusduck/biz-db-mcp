import pytest

from biz_db_mcp.errors import BizDbError
from biz_db_mcp.institution import parse_institution


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        ("0800001000", "00001000"),
        ("00001000", "00001000"),
        ("  0800001000 ", "00001000"),
        ("070012345", "0012345"),
        ("ABC123", "ABC123"),
        ("12", "12"),
        ("0000", "0000"),  # L = 0 is never a prefix
    ],
)
def test_normalization_matches_platform_rule(raw, code):
    assert parse_institution(raw).code == code


def test_raw_value_is_preserved():
    assert parse_institution(" 0800001000 ").raw == "0800001000"


@pytest.mark.parametrize("bad", [None, 800001000, "", "   ", "08\n00001000", "x" * 67])
def test_invalid_values_are_refused(bad):
    with pytest.raises(BizDbError):
        parse_institution(bad)
