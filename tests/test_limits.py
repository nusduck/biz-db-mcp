import pytest

from biz_db_mcp.errors import BUSY, RATE_LIMITED, BizDbError
from biz_db_mcp.limits import CallGate
from conftest import make_settings


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def _gate(**limits):
    clock = _Clock()
    return CallGate(make_settings({"limits": limits}).limits, clock=clock), clock


def test_rate_limit_is_per_institution_and_refills():
    gate, clock = _gate(rate={"per_minute": 60, "burst": 2})
    for _ in range(2):
        with gate.admit("A"):
            pass
    with pytest.raises(BizDbError) as excinfo:
        with gate.admit("A"):
            pass
    assert excinfo.value.code == RATE_LIMITED
    with gate.admit("B"):
        pass
    clock.now += 1.0
    with gate.admit("A"):
        pass


def test_concurrency_slots_refuse_instead_of_queueing():
    gate, _ = _gate(concurrency={"total": 2, "per_institution": 1, "export": 1, "import": 1})
    with gate.admit("A"):
        with pytest.raises(BizDbError) as excinfo:
            with gate.admit("A"):
                pass
        assert excinfo.value.code == BUSY
        with gate.admit("B", "export"):
            with pytest.raises(BizDbError):
                with gate.admit("C"):
                    pass
    with gate.admit("A"):
        pass


def test_slot_is_released_when_the_body_fails():
    gate, _ = _gate(concurrency={"total": 1, "per_institution": 1, "export": 1, "import": 1})
    with pytest.raises(RuntimeError):
        with gate.admit("A", "import"):
            raise RuntimeError("boom")
    with gate.admit("A", "import"):
        pass
