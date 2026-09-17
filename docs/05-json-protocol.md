# 05 - JSON protocol

For callers that prefer dictionaries and JSON over Python objects - queues,
sockets, HTTP handlers, other languages via a thin shim.

## Request

```json
{
  "type": "ipv4" | "ipv6" | "auto" | "list",
  "action": "info" | "distance",
  "data": {"type": "ipv4", "ip": "8.8.8.8", "list": ["1.1.1.1", "..."]},
  "status": ""
}
```

* `action` accepts aliases (`lookup`, `query`, `dist`, `信息`, `距离`); the
  response always echoes the canonical English name.
* `type` is a declaration and is enforced: `ipv4` with an IPv6 literal is an
  error, not a silent mismatch. `list` means "treat `data.list` as a batch".
* `data.ip` is the subject address; `data.list` (aliases: `ips`, `targets`) is
  the batch / target list. For `distance`, N is unbounded.

## Response

```json
{
  "type": "ipv4",
  "action": "info",
  "status": "ok" | "error",
  "data": {...},
  "error": null,
  "meta": {"schema_version": "1.0", "locales": ["en"], "databases": [...],
           "attribution": "...", "elapsed_ms": 0.12}
}
```

`data` for `action=info`: the standard document from
[03-output-schema.md](03-output-schema.md), or
`{"type": "list", "count": N, "results": [...]}` for batch input.

`data` for `action=distance`:

```json
{
  "type": "ipv4",
  "ip": "8.8.8.8",
  "source": { ...full standard document for the source IP... },
  "list": [ { ...per-pair Distance document... } ],
  "summary": {"count": 2, "resolved": 2, "min_km": 10091.0,
              "max_km": 11953.88, "avg_km": 11022.44}
}
```

`error` is populated only when `status == "error"`:

```json
{"code": "invalid_ip", "message": "...", "detail": "2001:4860:4860::8888"}
```

Stable codes: `invalid_ip`, `protocol_error`, `unsupported_action`,
`database_error`, `database_not_found`, `no_database`, `download_error`,
`internal_error`.

## Calling it

`info()` serves `action=info`, `distance()` serves `action=distance`:

```python
from detector import info, distance

response = await info({"type": "ipv4", "action": "info", "data": {"ip": "8.8.8.8"}})
response.ok                    # True
response.data["country"]["iso_code"]

await info('{"type":"auto","action":"info","data":{"ip":"1.1.1.1"}}')   # JSON string in
await info(envelope, as_dict=True)                                      # dict out
await distance({"type": "ipv4", "action": "distance",
                "data": {"ip": "8.8.8.8", "list": ["1.1.1.1"]}})

# low level, if you hold a client already
client = await AsyncDetector.create()
client.request(payload)        # -> Response
client.handle_json(payload)    # -> JSON string
```

Batch payloads: pass a list of request objects; you get one `action=batch`
response whose `data.results` mirrors the input order. Per-item failures degrade
to `found: false` plus an `error` object, so one bad row never kills the batch.

```python
request([
    {"type": "auto", "action": "info", "data": {"ip": "8.8.8.8"}},
    {"type": "auto", "action": "info", "data": {"ip": "not-an-ip"}},
])
```

## Error examples

```python
request({"type": "ipv4", "action": "info", "data": {"ip": "::1"}})
# status=error, code=invalid_ip  ("type=ipv4 does not match the actual IPv6 address")

request({"type": "auto", "action": "teleport", "data": {"ip": "8.8.8.8"}})
# status=error, code=unsupported_action, detail.supported == ["distance", "info"]

request({"type": "auto", "action": "distance", "data": {"ip": "8.8.8.8"}})
# status=error, code=protocol_error  ("distance requires data.list ...")
```

## Objects instead of dicts

```python
from detector import Request, Response

req = Request(type="ipv6", action="lookup", data={"ip": "2001:4860:4860::8888"})
req.resolved_action        # 'info'
req.resolved_type          # 'ipv6'
resp = detector.request(req)   # -> Response
resp.ok, resp.status, resp.action, resp.meta["elapsed_ms"]
```

## Async

Everything is async: `Response` is simply what those two coroutines return when
you hand them an envelope.
