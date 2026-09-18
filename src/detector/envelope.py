"""JSON protocol adapter.

Request (field names are kept exactly as specified)::

    {
      "type": "ipv4" | "ipv6" | "auto" | "list",
      "action": "info" | "distance",
      "data": {"type": "ipv4", "ip": "8.8.8.8", "list": ["1.1.1.1", "..."]},
      "status": ""
    }

Response::

    {
      "type": "ipv4",
      "action": "info",
      "status": "ok" | "error",
      "data": {...},
      "error": null,
      "meta": {"schema_version": "1.0", "elapsed_ms": 0.12, "databases": [...],
               "attribution": "IP Geolocation by DB-IP (https://db-ip.com)"}
    }

Notes:

* ``action`` accepts a few aliases for convenience, but the response always
  echoes the canonical English name (``info`` / ``distance`` / ``batch``).
* ``type`` is a *declaration* and is enforced: declaring ``ipv4`` while sending
  an IPv6 literal is an error, not a silent mismatch.
* Every payload is English-only: no localized strings are generated unless the
  caller explicitly asks for another ``locales`` order.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from .api import Detector, parse_ip
from .distance import Distance
from .exceptions import InvalidIPError, IPIntelError, ProtocolError, UnsupportedActionError
from .models import SCHEMA_VERSION, JsonModel

__all__ = ["Request", "Response", "handle", "error_response", "loads", "ACTION_ALIASES", "TYPE_ALIASES"]

#: Input-only aliases. Responses always use the canonical English name.
ACTION_ALIASES: Dict[str, str] = {
    "info": "info",
    "information": "info",
    "lookup": "info",
    "query": "info",
    "ip": "info",
    "信息": "info",
    "详情": "info",
    "查询": "info",
    "distance": "distance",
    "dist": "distance",
    "range": "distance",
    "距离": "distance",
}

TYPE_ALIASES: Dict[str, str] = {
    "ipv4": "ipv4",
    "4": "ipv4",
    "v4": "ipv4",
    "ip4": "ipv4",
    "ipv6": "ipv6",
    "6": "ipv6",
    "v6": "ipv6",
    "ip6": "ipv6",
    "auto": "auto",
    "any": "auto",
    "": "auto",
    "list": "list",
    "batch": "list",
}


def loads(payload: Union[str, bytes, Mapping[str, Any]]) -> Any:
    """Parse a request payload that may already be a dict/list."""
    if isinstance(payload, (dict, list)):
        return payload
    if isinstance(payload, (bytes, bytearray)):
        payload = bytes(payload).decode("utf-8", "replace")
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            raise ProtocolError("empty request payload")
        try:
            return json.loads(text)
        except ValueError as exc:
            raise ProtocolError(f"payload is not valid JSON: {exc}", detail=text[:200]) from exc
    raise ProtocolError(f"unsupported payload type: {type(payload).__name__}")


@dataclass
class Request(JsonModel):
    """Protocol request. Build it directly, or parse it from dict/JSON."""

    type: str = "auto"
    action: str = "info"
    data: Dict[str, Any] = field(default_factory=dict)
    status: str = ""

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "Request":
        if not isinstance(payload, Mapping):
            raise ProtocolError(f"request must be a JSON object, got {type(payload).__name__}")
        data = payload.get("data") or {}
        if not isinstance(data, Mapping):
            raise ProtocolError("field 'data' must be an object")
        return cls(
            type=str(payload.get("type") or "auto"),
            action=str(payload.get("action") or "info"),
            data=dict(data),
            status=str(payload.get("status") or ""),
        )

    @classmethod
    def from_json(cls, text: Union[str, bytes]) -> "Request":
        return cls.from_dict(loads(text))

    @property
    def resolved_action(self) -> str:
        key = (self.action or "").strip().lower()
        if key in ACTION_ALIASES:
            return ACTION_ALIASES[key]
        raise UnsupportedActionError(
            f"unsupported action: {self.action!r}",
            detail={
                "supported": sorted(set(ACTION_ALIASES.values())),
                "aliases": sorted(ACTION_ALIASES),
            },
        )

    @property
    def resolved_type(self) -> str:
        key = (self.type or "").strip().lower()
        if key in TYPE_ALIASES:
            return TYPE_ALIASES[key]
        raise ProtocolError(
            f"unsupported type: {self.type!r}", detail=sorted(set(TYPE_ALIASES.values()))
        )

    def to_dict(
        self,
        *,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> Dict[str, Any]:
        return {
            "type": self.type,
            "action": self.action,
            "data": dict(self.data),
            "status": self.status,
        }


@dataclass
class Response(JsonModel):
    """Protocol response with a stable structure."""

    type: str
    action: str
    status: str = "ok"
    data: Any = None
    error: Optional[Dict[str, Any]] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == "ok"

    def to_dict(
        self,
        *,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> Dict[str, Any]:
        return {
            "type": self.type,
            "action": self.action,
            "status": self.status,
            "data": self.data,
            "error": self.error,
            "meta": dict(self.meta),
        }


def _declared_version(request: Request) -> Optional[int]:
    declared = request.resolved_type
    if declared == "ipv4":
        return 4
    if declared == "ipv6":
        return 6
    return None


def _check_version(declared: Optional[int], value: Any) -> None:
    if declared is None:
        return
    addr = parse_ip(value)
    if addr.version != declared:
        name = "ipv4" if declared == 4 else "ipv6"
        raise InvalidIPError(
            f"type={name} does not match the actual IPv{addr.version} address: {addr}",
            detail=str(addr),
        )


def _extract_ips(data: Mapping[str, Any]) -> List[Any]:
    items: List[Any] = []
    single = data.get("ip")
    if single not in (None, ""):
        items.append(single)
    for key in ("list", "ips", "targets"):
        value = data.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (str, bytes)):
            items.append(value)
        elif isinstance(value, Sequence):
            items.extend(value)
        else:
            raise ProtocolError(f"data.{key} must be an array or a string")
    return items


def handle(detector: Detector, payload: Any) -> Response:
    """Main protocol entry point.

    ``payload`` may be a :class:`Request`, a ``dict``, a JSON string, or a list
    of any of those (batch mode).
    """
    started = time.perf_counter()

    if isinstance(payload, (list, tuple)):
        responses = [handle(detector, item) for item in payload]
        return Response(
            type="list",
            action="batch",
            status="ok" if all(item.ok for item in responses) else "error",
            data={"count": len(responses), "results": [item.to_dict() for item in responses]},
            meta=_meta(detector, started),
        )

    try:
        if isinstance(payload, Request):
            request = payload
        elif isinstance(payload, Mapping):
            request = Request.from_dict(payload)
        else:
            request = Request.from_dict(loads(payload))
    except IPIntelError as exc:
        return Response(
            type="auto", action="unknown", status="error", error=exc.to_dict(),
            meta=_meta(detector, started),
        )

    try:
        action = request.resolved_action
        declared = _declared_version(request)
        if action == "info":
            data = _handle_info(detector, request, declared)
        else:
            data = _handle_distance(detector, request, declared)
        return Response(
            type=request.resolved_type,
            action=action,
            status="ok",
            data=data,
            meta=_meta(detector, started),
        )
    except IPIntelError as exc:
        return Response(
            type=request.type, action=request.action, status="error", error=exc.to_dict(),
            meta=_meta(detector, started),
        )
    except Exception as exc:  # pragma: no cover - the protocol never raises
        return Response(
            type=request.type, action=request.action, status="error",
            error={"code": "internal_error", "message": str(exc)},
            meta=_meta(detector, started),
        )


def error_response(
    exc: IPIntelError,
    payload: Any = None,
    *,
    meta: Optional[Dict[str, Any]] = None,
) -> Response:
    """Protocol-shaped error for failures that happen *before* handling starts.

    Used when the database set is not ready yet (``loading_timeout``) or cannot be
    opened at all: the caller still gets ``{type, action, status, error, meta}``
    instead of an exception, so the envelope contract holds.
    """
    type_ = "auto"
    action = "unknown"
    try:
        if isinstance(payload, Request):
            request = payload
        elif isinstance(payload, Mapping):
            request = Request.from_dict(payload)
        else:
            request = Request.from_dict(loads(payload))
        type_, action = request.type, request.action
    except Exception:
        pass
    return Response(
        type=type_, action=action, status="error", error=exc.to_dict(), meta=dict(meta or {})
    )


def _handle_info(detector: Detector, request: Request, declared: Optional[int]) -> Any:
    requested_type = request.resolved_type
    items = _extract_ips(request.data)
    if not items:
        raise ProtocolError("provide data.ip or data.list")

    wants_list = requested_type == "list" or len(items) > 1
    if not wants_list:
        target = items[0]
        _check_version(declared, target)
        return detector.lookup(target).to_dict()

    results: List[Any] = []
    for item in items:
        try:
            _check_version(declared, item)
            results.append(detector.lookup(item).to_dict())
        except IPIntelError as exc:
            results.append({"ip": str(item), "found": False, "error": exc.to_dict()})
    return {"type": "list", "count": len(results), "results": results}


def _handle_distance(detector: Detector, request: Request, declared: Optional[int]) -> Any:
    data = request.data
    source = data.get("ip") or data.get("source") or data.get("from")
    if source in (None, ""):
        raise ProtocolError("distance requires data.ip as the source address")
    targets = _extract_ips({key: value for key, value in data.items() if key != "ip"})
    if not targets:
        raise ProtocolError("distance requires data.list with one or more targets")

    _check_version(declared, source)
    distances = detector.distance(source, targets)
    if isinstance(distances, Distance):
        distances = [distances]

    source_info = detector.lookup(source).to_dict()
    rows: List[Dict[str, Any]] = []
    values: List[float] = []
    for item in distances:
        rows.append(item.to_dict())
        if item.km is not None:
            values.append(float(item.km))

    summary = {
        "count": len(rows),
        "resolved": len(values),
        "min_km": round(min(values), 2) if values else None,
        "max_km": round(max(values), 2) if values else None,
        "avg_km": round(sum(values) / len(values), 2) if values else None,
    }
    return {
        "type": request.resolved_type,
        "ip": source_info["ip"],
        "source": source_info,
        "list": rows,
        "summary": summary,
    }


def _meta(detector: Detector, started: float) -> Dict[str, Any]:
    meta = detector._meta_for()
    meta["schema_version"] = SCHEMA_VERSION
    meta["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
    return meta
