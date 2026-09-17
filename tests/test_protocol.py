"""JSON protocol layer: the same two functions also accept envelopes."""

from __future__ import annotations

import asyncio
import json

import pytest

from detector import AsyncDetector, Detector, Request, Response, distance, info

from .conftest import V4_ALIBABA, V4_CHINA, V4_CLOUDFLARE, V4_GOOGLE, V6_GOOGLE


def run(payload, **kwargs):
    return asyncio.run(info(payload, **kwargs))


def test_info_envelope() -> None:
    response = run(
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


def test_action_aliases_resolve_to_english() -> None:
    for alias in ("info", "lookup", "query", "ip", "信息", "查询"):
        response = run({"type": "auto", "action": alias, "data": {"ip": V4_GOOGLE}})
        assert response.ok, alias
        assert response.action == "info"
    for alias in ("distance", "dist", "距离"):
        response = asyncio.run(
            distance({"type": "auto", "action": alias,
                      "data": {"ip": V4_GOOGLE, "list": [V4_CHINA]}})
        )
        assert response.ok, alias
        assert response.action == "distance"


def test_unknown_action_is_an_error() -> None:
    response = run({"type": "auto", "action": "explode", "data": {"ip": V4_GOOGLE}})
    assert response.status == "error"
    assert response.error["code"] == "unsupported_action"
    assert "info" in response.error["detail"]["supported"]


def test_type_declaration_is_enforced() -> None:
    response = run({"type": "ipv4", "action": "info", "data": {"ip": V6_GOOGLE}})
    assert response.status == "error"
    assert response.error["code"] == "invalid_ip"
    assert "ipv4" in response.error["message"]

    assert run({"type": "ipv6", "action": "info", "data": {"ip": V6_GOOGLE}}).ok


def test_info_list_form() -> None:
    response = run(
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


def test_distance_envelope() -> None:
    response = asyncio.run(
        distance(
            {
                "type": "ipv4",
                "action": "distance",
                "data": {"type": "ipv4", "ip": V4_GOOGLE, "list": [V4_CLOUDFLARE, V4_ALIBABA]},
            }
        )
    )
    assert response.ok
    payload = response.data
    assert payload["ip"] == V4_GOOGLE
    assert payload["source"]["country"]["iso_code"] == "US"
    assert len(payload["list"]) == 2
    summary = payload["summary"]
    assert summary["count"] == 2
    assert summary["min_km"] <= summary["avg_km"] <= summary["max_km"]


def test_distance_envelope_requires_targets() -> None:
    response = asyncio.run(
        distance({"type": "auto", "action": "distance", "data": {"ip": V4_GOOGLE}})
    )
    assert response.status == "error"
    assert response.error["code"] == "protocol_error"


def test_batch_envelope() -> None:
    response = run(
        [
            {"type": "auto", "action": "info", "data": {"ip": V4_GOOGLE}},
            {"type": "auto", "action": "distance", "data": {"ip": V4_GOOGLE, "list": [V4_CHINA]}},
        ]
    )
    assert response.action == "batch"
    assert response.ok
    assert response.data["count"] == 2


def test_json_string_envelope() -> None:
    text = '{"type":"auto","action":"info","data":{"ip":"8.8.8.8"}}'
    assert run(text, as_dict=True)["status"] == "ok"

    response = run(text)
    assert response.ok and response.data["ip"] == "8.8.8.8"


def test_malformed_payloads() -> None:
    """Envelope misuse produces a structured error; a bad address still raises."""
    assert run('{"action":"info"}').status == "error"            # no ip/list
    assert run({"action": "info", "data": []}).status == "error"  # data must be an object
    assert run({"action": "info", "data": {"ip": ""}}).status == "error"

    from detector import InvalidIPError

    with pytest.raises(InvalidIPError):
        run("not json")          # plain string -> treated as an address


def test_request_object_helpers() -> None:
    request = Request.from_json('{"type":"ipv4","action":"lookup","data":{"ip":"8.8.8.8"}}')
    assert request.resolved_action == "info"
    assert request.resolved_type == "ipv4"
    assert request.to_dict()["data"] == {"ip": "8.8.8.8"}

    typed = Request(type="6", action="距离", data={"ip": "::1", "list": ["::2"]})
    assert typed.resolved_type == "ipv6"
    assert typed.resolved_action == "distance"


def test_detector_methods_still_expose_the_protocol(detector: Detector) -> None:
    """The classes keep the low-level entry points for callers who want them."""
    response = detector.request({"type": "auto", "action": "info", "data": {"ip": V4_GOOGLE}})
    assert response.ok
    assert json.loads(detector.handle_json('{"type":"auto","action":"info","data":{"ip":"1.1.1.1"}}'))[
        "status"
    ] == "ok"


def test_async_client_request() -> None:
    async def scenario() -> None:
        async with await AsyncDetector.create(cache_size=0) as client:
            response = await client.request(
                {"type": "auto", "action": "info", "data": {"ip": V4_GOOGLE}}
            )
            assert response.ok
            assert json.loads(
                await client.request_json(
                    '{"type":"auto","action":"distance","data":{"ip":"8.8.8.8","list":["1.1.1.1"]}}'
                )
            )["status"] == "ok"

    asyncio.run(scenario())


def test_non_object_json_payloads() -> None:
    assert run("[1,2,3]").status == "error"      # JSON array -> batch of non-envelopes


@pytest.mark.parametrize("bad", ["42", "null", ""])
def test_non_address_strings_raise(bad: str) -> None:
    from detector import InvalidIPError

    with pytest.raises(InvalidIPError):
        run(bad)
