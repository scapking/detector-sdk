"""Everything the SDK does, with the real returned payloads printed.

    python examples/show_output.py
"""

from __future__ import annotations

import asyncio
import json

import detector
from detector import Detector, distance, info, known_datasets

LINE = "=" * 78


def section(title: str) -> None:
    print(f"\n{LINE}\n{title}\n{LINE}")


async def main() -> None:
    section(f"1. version and bundled datasets (detector {detector.__version__})")
    for key, entry in known_datasets().items():
        provides = ",".join(entry["providers"]) or entry["kind"]
        print(f"  {key:<18}{entry['name']:<20}{provides[:24]:<26}"
              f"{entry['license']:<24}{len(entry['files'])} file(s), {entry['update']}")
    client = Detector()
    print(f"\nloaded: {len(client.databases)} files / {len(client.dataset_keys)} datasets")

    section("2. the public surface: two functions (+ configure/close)")
    import inspect

    funcs = sorted(n for n in detector.__all__ if inspect.isfunction(getattr(detector, n, None)))
    print("  ", ", ".join(funcs))

    section("3. info() - one IP, standard JSON (lean mode)")
    lean = await info("8.8.8.8", include_raw=False, include_all_names=False, as_dict=True)
    print(json.dumps(lean, ensure_ascii=False, indent=2)[:2600], "...")

    section("4. info() - batch (same function, list or generator)")
    for row in await info(["8.8.8.8", "1.1.1.1", "114.114.114.114", "2001:4860:4860::8888"]):
        print(f"  {row.ip:<26}{row.country_code!s:<4}{row.city_name!s:<16}"
              f"{row.asn.asn:<10}{row.asn.organization}")

    section("5. per-database raw records (info(...).raw, default mode)")
    full = await info("8.8.8.8")
    for key, record in full.raw.items():
        text = json.dumps(record, ensure_ascii=False)
        print(f"  {key:<22}{text[:120]}{'…' if len(text) > 120 else ''}")

    section("6. cross-check: what every database says about the same IP")
    print("  country   :", json.dumps(full.cross_check["country"], ensure_ascii=False))
    print("  location  :", json.dumps(full.cross_check["location"], ensure_ascii=False))
    print("  asn_org   :", json.dumps(full.cross_check["asn_organization"], ensure_ascii=False))
    print("  agreement :", json.dumps(full.agreement))
    print("  conflicts :", full.conflicts)

    section("7. private address and IPv6")
    private = await info("192.168.1.1", include_raw=False)
    print(f"  192.168.1.1 -> found={private.found} is_private={private.is_private} "
          f"iso={private.country}")
    v6 = await info("2001:4860:4860::8888", include_raw=False)
    print(f"  v6 -> {v6.country_code} {v6.city_name} {v6.coordinates} "
          f"network={v6.network} sources={sorted(v6.sources)[:3]}…")

    section("8. distance() - 1-to-1, 1-to-N, N-to-M")
    print("  1-to-1:", json.dumps((await distance("8.8.8.8", "1.1.1.1")).to_dict(), ensure_ascii=False))
    for row in await distance("8.8.8.8", ["223.5.5.5", "2001:4860:4860::8888"]):
        print("  1-to-N:", json.dumps(row.to_dict(), ensure_ascii=False)[:220], "…")
    for row in await distance(["8.8.8.8", "1.1.1.1"], ["223.5.5.5"]):
        print(f"  N-to-M: {row.source} -> {row.target} {row.km:,.1f} km")

    section("9. the same functions also take JSON envelopes")
    response = await distance({"type": "ipv4", "action": "distance",
                               "data": {"ip": "8.8.8.8", "list": ["1.1.1.1"]}})
    print("  status:", response.status, "| action:", response.action,
          "| summary:", json.dumps(response.data["summary"]))
    error = await info({"type": "ipv4", "action": "info", "data": {"ip": "::1"}})
    print("  error shape:", json.dumps(error.error, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
