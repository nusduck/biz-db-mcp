"""DBPM password provider boundary.

UPspec documents a private Java ``DBPMClientAPI``. This Python service does
not vendor that SDK, so enabled mode invokes a locally installed bridge
command. The bridge receives a JSON request on stdin and must write only the
resolved password to stdout. DBPM app-key material remains in the process
environment and is inherited by the bridge, never placed in arguments.
"""

from __future__ import annotations

import json
import os
import subprocess
from typing import Protocol

from .config import DbpmSettings


class DbpmError(RuntimeError):
    """Raised when DBPM password resolution fails."""


class PasswordProvider(Protocol):
    def get_password(self, database: str, username: str) -> str:
        """Resolve one database credential without logging the password."""


class CommandPasswordProvider:
    """Adapt the private DBPM client through a local executable."""

    def __init__(self, settings: DbpmSettings) -> None:
        if not settings.enabled or not settings.command:
            raise DbpmError("DBPM provider is not enabled")
        self.settings = settings

    def get_password(self, database: str, username: str) -> str:
        request = {
            "database": database,
            "username": username,
            "servers": list(self.settings.servers),
            "app_name": self.settings.app_name,
            "app_ip": self.settings.app_ip,
            "connect_timeout_ms": self.settings.connect_timeout_ms,
            "read_timeout_ms": self.settings.read_timeout_ms,
        }
        bridge_env = os.environ.copy()
        # These names make the bridge straightforward to implement around
        # DBPMClientAPI.doGetPasswd while keeping the private key out of argv.
        bridge_env.update(
            {
                "DBPM_SERVERS": ",".join(self.settings.servers),
                "DBPM_APP_NAME": self.settings.app_name or "",
                "DBPM_APP_KEY": self.settings.app_key or "",
                "DBPM_APP_IP": self.settings.app_ip or "",
            }
        )

        try:
            completed = subprocess.run(
                [self.settings.command],
                input=json.dumps(request, separators=(",", ":")) + "\n",
                text=True,
                capture_output=True,
                check=False,
                timeout=self.settings.command_timeout_seconds,
                env=bridge_env,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise DbpmError("DBPM password provider could not be executed") from exc

        if completed.returncode != 0:
            raise DbpmError("DBPM password provider returned a failure")

        password = completed.stdout.rstrip("\r\n")
        if not password or "\n" in password or "\r" in password:
            raise DbpmError("DBPM password provider returned an invalid password")
        return password
