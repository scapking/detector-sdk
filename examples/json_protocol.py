"""The JSON envelope: both functions accept it; the reply is the standard document.

    python examples/json_protocol.py
"""

from __future__ import annotations

import asyncio
import json

from detector import distance, info


def dump(document: dict) -> None:
    print(f"-> status={document['status']} action={document['action']}")
    body = dict(document)
    body["meta"] = {key: value for key, value in body["meta"].items() if key != "databases"}
    print(json.dumps(body, ensure_ascii=False, indent=2)[:800])
    print()


async def main() -> None:
    # info() serves information envelopes
    dump(await info({"type": "ipv4", "action": "info", "data": {"type": "ipv4", "ip": "8.8.8.8"}}))

    # distance() serves distance envelopes
    dump(await distance({"type": "ipv4", "action": "distance",
                         "data": {"type": "ipv4", "ip": "8.8.8.8", "list": ["1.1.1.1"]}}))

    # Batch payload: several requests in one call
    dump(await info([
        {"type": "auto", "action": "info", "data": {"ip": "8.8.8.8"}},
        {"type": "ipv6", "action": "info", "data": {"ip": "2001:4860:4860::8888"}},
    ]))

    # Errors are data, not exceptions
    dump(await info({"type": "ipv4", "action": "info", "data": {"ip": "2001:4860:4860::8888"}}))
    dump(await info({"type": "auto", "action": "teleport", "data": {"ip": "8.8.8.8"}}))

    # JSON strings work too; as_object=True hands back the Response model instead
    text = '{"type":"auto","action":"info","data":{"ip":"1.1.1.1"}}'
    print("json string ->", (await info(text))["data"]["display"])
    response = await info(text, as_object=True)
    print("as_object   ->", type(response).__name__, response.status, response.ok)


if __name__ == "__main__":
    asyncio.run(main())
