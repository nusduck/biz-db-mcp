import json
import logging
import os
import time

import pytest

from biz_db_mcp.audit import log_call
from biz_db_mcp.config import ConfigError
from biz_db_mcp.export import TEMP_PREFIX, prepare_temp_dir
from biz_db_mcp.logs import AUDIT_LOGGER, configure_logging
from conftest import make_settings


@pytest.fixture(autouse=True)
def _restore_logging():
    audit = logging.getLogger(AUDIT_LOGGER)
    root = logging.getLogger()
    saved = (list(audit.handlers), list(root.handlers), root.level)
    yield
    for logger in (audit, root):
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
    audit.handlers[:] = saved[0]
    root.handlers[:] = saved[1]
    root.setLevel(saved[2])


def test_log_paths_resolve_relative_to_config_dir(tmp_path):
    settings = make_settings({"logging": {"audit": {"path": "logs/audit.log"}}})
    assert os.path.isabs(settings.logging.audit.path)


def test_logging_config_validation():
    with pytest.raises(ConfigError, match="backup_count must be >= 1"):
        make_settings({"logging": {"audit": {"path": "/tmp/a.log", "backup_count": 0}}})
    with pytest.raises(ConfigError, match="audit.path is required"):
        make_settings({"logging": {"stderr": False}})
    with pytest.raises(ConfigError, match="must differ"):
        make_settings({"logging": {"audit": {"path": "/tmp/x.log"}, "app": {"path": "/tmp/x.log"}}})
    with pytest.raises(ConfigError, match="level"):
        make_settings({"logging": {"level": "LOUD"}})


def test_audit_file_rotates_within_its_size_bound(tmp_path, capsys):
    path = tmp_path / "logs" / "audit.log"
    settings = make_settings({
        "logging": {"stderr": False, "audit": {"path": str(path), "max_bytes": 65536, "backup_count": 2}},
    })
    configure_logging(settings.logging)
    for _ in range(3000):
        with log_call("query", sql="SELECT secret_column FROM t", database="primary"):
            pass

    files = sorted(p.name for p in path.parent.iterdir())
    assert files == ["audit.log", "audit.log.1", "audit.log.2"]
    total = sum(p.stat().st_size for p in path.parent.iterdir())
    assert total <= settings.logging.audit.max_disk_bytes
    record = json.loads(path.read_text().splitlines()[-1])
    assert record["tool"] == "query" and "secret_column" not in path.read_text()
    assert oct(path.stat().st_mode & 0o777) == "0o640"
    assert capsys.readouterr().err == ""


def test_app_logs_respect_level_and_stay_out_of_audit(tmp_path):
    audit = tmp_path / "audit.log"
    app = tmp_path / "server.log"
    settings = make_settings({
        "logging": {"stderr": False, "level": "WARNING",
                    "audit": {"path": str(audit)}, "app": {"path": str(app)}},
    })
    configure_logging(settings.logging)
    logging.getLogger("mcp.server").info("noisy request log")
    logging.getLogger("biz_db_mcp.server").warning("important")
    with log_call("list_databases"):
        pass
    for handler in logging.getLogger().handlers + logging.getLogger(AUDIT_LOGGER).handlers:
        handler.flush()
    assert "important" in app.read_text() and "noisy" not in app.read_text()
    assert "list_databases" in audit.read_text() and "list_databases" not in app.read_text()


def test_prepare_temp_dir_removes_only_stale_own_files(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    stale = staging / f"{TEMP_PREFIX}old.csv"
    fresh = staging / f"{TEMP_PREFIX}new.csv"
    other = staging / "keep.csv"
    for p in (stale, fresh, other):
        p.write_text("x")
    old = time.time() - 7200
    os.utime(stale, (old, old))
    os.utime(other, (old, old))

    assert prepare_temp_dir(str(staging), stale_seconds=3600) == 1
    assert not stale.exists() and fresh.exists() and other.exists()
    assert prepare_temp_dir(str(tmp_path / "created"), stale_seconds=3600) == 0
    assert (tmp_path / "created").is_dir()
