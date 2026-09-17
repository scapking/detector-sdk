"""Everything the SDK does, with the real returned payloads printed.

    python examples/show_output.py
"""

from __future__ import annotations

import asyncio
import inspect
import json

import detector
from detector import AsyncDetector, distance, info, known_datasets

LINE = "=" * 78


def section(title: str) -> None:
    print(f"\n{LINE}\n{title}\n{LINE}")


async def main() -> None:
    section(f"1. version and bundled datasets (detector {detector.__version__})")
    for key, entry in known_datasets().items():
        provides = ",".join(entry["providers"]) or entry["kind"]
        print(f"  {key:<18}{entry['name']:<20}{provides[:24]:<26}"
              f"{entry['license']:<24}{len(entry['files'])} file(s), {entry['update']}")
    async with await AsyncDetector.create() as client:
        print(f"\nloaded: {len(client.databases)} files / {len(client.dataset_keys)} datasets")

    section("2. the public surface: two functions (+ configure/close)")
    funcs = sorted(n for n in detector.__all__ if inspect.isfunction(getattr(detector, n, None)))
    print("  ", ", ".join(funcs))

    section("3. info() - one IP, standard JSON (this is the default return value)")
    document = await info("8.8.8.8", include_raw=False, include_all_names=False)
    print(type(document).__name__, "-", len(document), "top-level keys")
    print(json.dumps(document, ensure_ascii=False, indent=2)[:2200], "...")

    section("4. info() - batch (same function, list or generator)")
    for row in await info(["8.8.8.8", "1.1.1.1", "114.114.114.114", "2001:4860:4860::8888"]):
        country = row["country"]["iso_code"] if row["country"] else "-"
        city = row["city"]["name"] if row["city"] else "-"
        print(f"  {row['ip']:<26}{country:<4}{city:<16}{row['asn']['asn']:<9}"
              f"{row['asn']['organization']}")

    section("5. per-database raw records (info(..., as_object=True).raw)")
    model = await info("8.8.8.8", as_object=True)
    for key, record in model.raw.items():
        text = json.dumps(record, ensure_ascii=False)
        print(f"  {key:<22}{text[:110]}{'...' if len(text) > 110 else ''}")

    section("6. cross-check: what every database says about the same address")
    print("  country   :", json.dumps(model.cross_check["country"], ensure_ascii=False))
    print("  location  :", json.dumps(model.cross_check["location"], ensure_ascii=False))
    print("  asn_org   :", json.dumps(model.cross_check["asn_organization"], ensure_ascii=False))
    print("  agreement :", json.dumps(model.agreement))
    print("  conflicts :", model.conflicts)

    section("7. private address and IPv6")
    private = await info("192.168.1.1", include_raw=False)
    print(f"  192.168.1.1 -> found={private['found']} "
          f"is_private={private['flags']['is_private']} country={private['country']}")
    v6 = await info("2001:4860:4860::8888", include_raw=False)
    print(f"  v6 -> {v6['country']['iso_code']} {v6['city']['name']} "
          f"({v6['location']['latitude']}, {v6['location']['longitude']}) "
          f"network={v6['network']} sources={len(v6['sources'])}")

    section("8. distance() - 1-to-1, 1-to-N, N-to-M (all plain dicts)")
    print("  1-to-1:", json.dumps(await distance("8.8.8.8", "1.1.1.1"), ensure_ascii=False))
    for row in await distance("8.8.8.8", ["223.5.5.5", "2001:4860:4860::8888"]):
        print("  1-to-N:", json.dumps(row, ensure_ascii=False)[:190], "...")
    for row in await distance(["8.8.8.8", "1.1.1.1"], ["223.5.5.5"]):
        print(f"  N-to-M: {row['source']} -> {row['target']} {row['distance_km']:,.1f} km")

    section("9. the same functions also take JSON envelopes")
    response = await distance({"type": "ipv4", "action": "distance",
                               "data": {"ip": "8.8.8.8", "list": ["1.1.1.1"]}})
    print("  status:", response["status"], "| action:", response["action"],
          "| summary:", json.dumps(response["data"]["summary"]))
    error = await info({"type": "ipv4", "action": "info", "data": {"ip": "::1"}})
    print("  error shape:", json.dumps(error["error"], ensure_ascii=False))

    section("10. as_object=True hands back the models")
    model = await info("8.8.8.8", as_object=True)
    print(f"  {type(model).__name__}  {model.country.iso_code}  {model.city.name()}  "
          f"{model.asn.asn}  {model.coordinates}")
    row = await distance("8.8.8.8", "1.1.1.1", as_object=True)
    print(f"  {type(row).__name__}  {row.km:,.2f} km  {row.mi:,.2f} mi  {row!s}")


if __name__ == "__main__":
    asyncio.run(main())
