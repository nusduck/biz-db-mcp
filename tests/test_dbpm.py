from biz_db_mcp.config import DbpmSettings
from biz_db_mcp.dbpm import CommandPasswordProvider


def test_command_provider_passes_request_on_stdin_and_returns_only_stdout(tmp_path):
    bridge = tmp_path / "dbpm-bridge"
    bridge.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "request = json.load(sys.stdin)\n"
        "assert request['database'] == 'test'\n"
        "assert request['username'] == 'cup_dba'\n"
        "assert os.environ['DBPM_APP_KEY'] == 'file:/tmp/key'\n"
        "print('resolved-password')\n"
    )
    bridge.chmod(0o700)
    settings = DbpmSettings(
        enabled=True,
        command=str(bridge),
        servers=("127.0.0.1:7000",),
        app_name="APP",
        app_key="file:/tmp/key",
        app_ip="127.0.0.1",
        connect_timeout_ms=5_000,
        read_timeout_ms=5_000,
        command_timeout_seconds=5,
    )

    password = CommandPasswordProvider(settings).get_password("test", "cup_dba")

    assert password == "resolved-password"
