"""The JSON envelope: request in, standard JSON out, errors included.

    python examples/json_protocol.py
"""

from __future__ import annotations

import json

from detector import request, request_json


def show(payload: object) -> None:
    response = request(payload)
    print(f"-> status={response.status} action={response.action}")
    print(json.dumps(response.to_dict(), indent=2)[:900])
    print()


def main() -> None:
    # 1. Single IP information.
    show({"type": "ipv4", "action": "info", "data": {"type": "ipv4", "ip": "8.8.8.8"}})

    # 2. 1-to-N distance with the source record included.
    show(
        {
            "type": "ipv4",
            "action": "distance",
            "data": {"type": "ipv4", "ip": "8.8.8.8", "list": ["1.1.1.1", "223.5.5.5"]},
        }
    )

    # 3. Batch: several requests in one payload.
    show(
        [
            {"type": "auto", "action": "info", "data": {"ip": "8.8.8.8"}},
            {"type": "ipv6", "action": "info", "data": {"ip": "2001:4860:4860::8888"}},
        ]
    )

    # 4. Error paths are data, not exceptions.
    show({"type": "ipv4", "action": "info", "data": {"ip": "2001:4860:4860::8888"}})  # wrong family
    show({"type": "auto", "action": "teleport", "data": {"ip": "8.8.8.8"}})          # unknown action
    show({"type": "auto", "action": "info", "data": {"ip": "999.999.999.999"}})       # bad address

    # 5. JSON string round trip - handy for queues and sockets.
    text = request_json('{"type":"auto","action":"info","data":{"ip":"1.1.1.1"}}')
    print("request_json ->", json.loads(text)["status"])


if __name__ == "__main__":
    main()
