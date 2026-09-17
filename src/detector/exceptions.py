"""Exception types.

Every exception carries a stable ``code``; the JSON protocol reuses it for the
``status="error"`` response so callers never have to parse a human-readable
message.
"""

from __future__ import annotations

from typing import Any, Dict

__all__ = [
    "IPIntelError",
    "InvalidIPError",
    "DatabaseError",
    "DatabaseNotFoundError",
    "NoDatabaseError",
    "DistanceUnavailableError",
    "UnsupportedActionError",
    "ProtocolError",
    "DownloadError",
]


class IPIntelError(Exception):
    """Base class for every detector error."""

    code = "error"

    def __init__(self, message: str = "", *, detail: Any = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.detail = detail

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {"code": self.code, "message": self.message}
        if self.detail is not None:
            data["detail"] = self.detail
        return data

    def __str__(self) -> str:  # pragma: no cover - trivial passthrough
        return f"[{self.code}] {self.message}"


class InvalidIPError(IPIntelError):
    """Input is not a valid IPv4 / IPv6 address."""

    code = "invalid_ip"


class DatabaseError(IPIntelError):
    """Database read failure."""

    code = "database_error"


class DatabaseNotFoundError(DatabaseError):
    """The requested .mmdb file is missing or unreadable."""

    code = "database_not_found"


class NoDatabaseError(DatabaseError):
    """No usable database: bundled data missing and no external file given."""

    code = "no_database"


class DistanceUnavailableError(IPIntelError):
    """Coordinates are missing, so distance cannot be computed."""

    code = "distance_unavailable"


class UnsupportedActionError(IPIntelError):
    """The request contains an unknown ``action``."""

    code = "unsupported_action"


class ProtocolError(IPIntelError):
    """The request envelope is malformed."""

    code = "protocol_error"


class DownloadError(IPIntelError):
    """Database download / update failed."""

    code = "download_error"
