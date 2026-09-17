"""JSON protocol layer: request/response envelopes, aliases, error handling."""

from __future__ import annotations

import json

from detector import Detector, Request, Response

from .conftest import V4_ALIBABA, V4_CHINA, V4_CLOUDFLARE, V4_GOOGLE, V6_GOOGLE


def test_info_envelope(detector: Detector) -> None:
    response = detector.request(
        {"type": "ipv4", "action": "info", "data": {"type": "ipv4", "ip": V4_GOOGLE}, "status": ""}
    )
    assert isinstance(response, Response)
    assert response.ok
    assert response.status == "ok"
    assert response.action == "info"
    assert response.type == "ipv4"
    assert response.data["ip"] == V4_GOOGLE
    assert response.data["country"]["iso_code"] == "US"
    assert response.error is None
    assert response.meta["elapsed_ms"] >= 0
    assert set(response.to_dict()) == {"type", "action", "status", "data", "error", "meta"}


def test_action_aliases_resolve_to_english(detector: Detector) -> None:
    for alias in ("info", "lookup", "query", "ip", "信息", "查询"):
        response = detector.request({"type": "auto", "action": alias, "data": {"ip": V4_GOOGLE}})
        assert response.ok, alias
        assert response.action == "info"
    for alias in ("distance", "dist", "距离"):
        response = detector.request(
            {"type": "auto", "action": alias, "data": {"ip": V4_GOOGLE, "list": [V4_CHINA]}}
        )
        assert response.ok, alias
        assert response.action == "distance"


def test_unknown_action_is_an_error(detector: Detector) -> None:
    response = detector.request({"type": "auto", "action": "explode", "data": {"ip": V4_GOOGLE}})
    assert response.status == "error"
    assert response.error["code"] == "unsupported_action"
    assert "info" in response.error["detail"]["supported"]


def test_type_declaration_is_enforced(detector: Detector) -> None:
    response = detector.request({"type": "ipv4", "action": "info", "data": {"ip": V6_GOOGLE}})
    assert response.status == "error"
    assert response.error["code"] == "invalid_ip"
    assert "ipv4" in response.error["message"]

    ok = detector.request({"type": "ipv6", "action": "info", "data": {"ip": V6_GOOGLE}})
    assert ok.ok


def test_info_list_form(detector: Detector) -> None:
    response = detector.request(
        {
            "type": "list",
            "action": "info",
            "data": {"type": "list", "list": [V4_GOOGLE, V4_CLOUDFLARE, "bad", V6_GOOGLE]},
        }
    )
    assert response.ok
    assert response.data["type"] == "list"
    assert response.data["count"] == 4
    assert response.data["results"][0]["country"]["iso_code"] == "US"
    assert response.data["results"][2]["found"] is False
    assert response.data["results"][2]["error"]["code"] == "invalid_ip"


def test_distance_envelope(detector: Detector) -> None:
    response = detector.request(
        {
            "type": "ipv4",
            "action": "distance",
            "data": {"type": "ipv4", "ip": V4_GOOGLE, "list": [V4_CLOUDFLARE, V4_ALIBABA]},
        }
    )
    assert response.ok
    payload = response.data
    assert payload["ip"] == V4_GOOGLE
    assert payload["source"]["country"]["iso_code"] == "US"
    assert len(payload["list"]) == 2
    summary = payload["summary"]
    assert summary["count"] == 2
    assert summary["min_km"] <= summary["avg_km"] <= summary["max_km"]


def test_distance_requires_targets(detector: Detector) -> None:
    response = detector.request({"type": "auto", "action": "distance", "data": {"ip": V4_GOOGLE}})
    assert response.status == "error"
    assert response.error["code"] == "protocol_error"


def test_batch_envelope(detector: Detector) -> None:
    response = detector.request(
        [
            {"type": "auto", "action": "info", "data": {"ip": V4_GOOGLE}},
            {"type": "auto", "action": "distance", "data": {"ip": V4_GOOGLE, "list": [V4_CHINA]}},
        ]
    )
    assert response.action == "batch"
    assert response.ok
    assert response.data["count"] == 2


def test_json_string_round_trip(detector: Detector) -> None:
    payload = json.dumps(
        {"type": "auto", "action": "info", "data": {"ip": V4_GOOGLE}, "status": ""}
    )
    text = detector.handle_json(payload)
    parsed = json.loads(text)
    assert parsed["status"] == "ok"
    assert parsed["data"]["ip"] == V4_GOOGLE


def test_malformed_payloads(detector: Detector) -> None:
    assert detector.request("not json").status == "error"
    assert detector.request("{}").status == "error"          # no ip/list
    assert detector.request({"action": "info", "data": []}).status == "error"
    assert detector.request({"action": "info", "data": {"ip": ""}}).status == "error"


def test_request_object_helpers() -> None:
    request = Request.from_json('{"type":"ipv4","action":"lookup","data":{"ip":"8.8.8.8"}}')
    assert request.resolved_action == "info"
    assert request.resolved_type == "ipv4"
    assert request.to_dict()["data"] == {"ip": "8.8.8.8"}

    typed = Request(type="6", action="距离", data={"ip": "::1", "list": ["::2"]})
    assert typed.resolved_type == "ipv6"
    assert typed.resolved_action == "distance"


def test_module_level_request_json() -> None:
    from detector import request, request_json

    response = request({"type": "auto", "action": "info", "data": {"ip": V4_GOOGLE}})
    assert response.ok
    text = request_json('{"type":"auto","action":"info","data":{"ip":"8.8.8.8"}}')
    assert json.loads(text)["status"] == "ok"
