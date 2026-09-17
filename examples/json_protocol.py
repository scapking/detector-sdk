"""The JSON envelope: both functions accept it, JSON in / standard JSON out.

    python examples/json_protocol.py
"""

from __future__ import annotations

import asyncio
import json

from detector import distance, info


def dump(response) -> None:
    print(f"-> status={response.status} action={response.action}")
    body = response.to_dict()
    body["meta"] = {key: value for key, value in body["meta"].items() if key != "databases"}
    print(json.dumps(body, ensure_ascii=False, indent=2)[:900])
    print()


async def main() -> None:
    # info() handles information envelopes
    dump(await info({"type": "ipv4", "action": "info", "data": {"type": "ipv4", "ip": "8.8.8.8"}}))

    # distance() handles distance envelopes
    dump(await distance({"type": "ipv4", "action": "distance",
                         "data": {"type": "ipv4", "ip": "8.8.8.8", "list": ["1.1.1.1", "223.5.5.5"]}}))

    # Batch payload: several requests in one call
    dump(await info([
        {"type": "auto", "action": "info", "data": {"ip": "8.8.8.8"}},
        {"type": "ipv6", "action": "info", "data": {"ip": "2001:4860:4860::8888"}},
    ]))

    # Errors are data, not exceptions
    dump(await info({"type": "ipv4", "action": "info", "data": {"ip": "2001:4860:4860::8888"}}))
    dump(await info({"type": "auto", "action": "teleport", "data": {"ip": "8.8.8.8"}}))

    # JSON strings work too, and as_dict=True skips the Response object
    text = '{"type":"auto","action":"info","data":{"ip":"1.1.1.1"}}'
    print("json string ->", (await info(text)).data["display"])
    print("as dict     ->", (await info(text, as_dict=True))["status"])


if __name__ == "__main__":
    asyncio.run(main())
