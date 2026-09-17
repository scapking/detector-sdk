"""Everything this SDK does, with the real returned payloads printed.

    python examples/show_output.py
"""

from __future__ import annotations

import json

import detector
from detector import Detector, distance, lookup, known_datasets, request_json

LINE = "=" * 78


def section(title: str) -> None:
    print(f"\n{LINE}\n{title}\n{LINE}")


def main() -> None:
    section(f"1. 版本与内置数据集 (detector {detector.__version__})")
    registry = known_datasets()
    print(f"{'键':<18}{'名称':<20}{'内容':<28}{'许可':<22}{'文件':<6}{'更新'}")
    for key, entry in registry.items():
        provides = ",".join(entry["providers"]) or entry["kind"]
        print(
            f"{key:<18}{entry['name']:<20}{provides[:26]:<28}"
            f"{entry['license']:<22}{len(entry['files']):<6}{entry['update']}"
        )
    loaded = Detector()
    print(f"\n已加载: {len(loaded.databases)} 个文件 / {len(loaded.dataset_keys)} 个数据集")

    section("2. 全部可调用项 (from detector import ...)")
    functions = [name for name in detector.__all__ if callable(getattr(detector, name, None))]
    print("函数/类:", ", ".join(sorted(functions)))

    section("3. 查询一个 IPv4 —— 标准 JSON（精简模式：不带 raw/多余语言）")
    lean = Detector(include_raw=False, include_all_names=False)
    print(lean.lookup("8.8.8.8").to_json(indent=2))

    section("4. 同一次查询里每个数据库各自返回的原始记录 (info.raw)")
    info = lookup("8.8.8.8")
    for key, record in info.raw.items():
        text = json.dumps(record, ensure_ascii=False)
        print(f"  {key:<22} {text[:150]}{'…' if len(text) > 150 else ''}")
    print("\n  sources:", json.dumps(info.sources, ensure_ascii=False))
    print("  cross_check:", json.dumps(info.cross_check, ensure_ascii=False)[:400], "…")

    section("5. IPv6 与私有地址")
    print("IPv6 2001:4860:4860::8888 ->",
          json.dumps(lean.lookup("2001:4860:4860::8888").to_dict(), ensure_ascii=False)[:300], "…")
    print("\n私有 192.168.1.1 ->")
    print(lean.lookup("192.168.1.1").to_json(indent=2))

    section("6. 距离 1 -> N")
    rows = distance("8.8.8.8", ["1.1.1.1", "223.5.5.5", "2001:4860:4860::8888"])
    for row in rows:
        print(json.dumps(row.to_dict(), ensure_ascii=False))

    section("7. JSON 协议入口（请求/响应信封）")
    payload = '{"type":"ipv4","action":"distance","data":{"ip":"8.8.8.8","list":["1.1.1.1"]}}'
    response = json.loads(request_json(payload))
    print("请求:", payload)
    print("响应:", json.dumps(
        {"type": response["type"], "action": response["action"], "status": response["status"],
         "error": response["error"],
         "data": {"ip": response["data"]["ip"],
                  "list": response["data"]["list"],
                  "summary": response["data"]["summary"]}},
        ensure_ascii=False, indent=2)[:1200], "…")
    print("meta keys:", list(response["meta"]))

    section("8. 错误也是结构化输出（不抛异常）")
    print(json.dumps(json.loads(request_json(
        '{"type":"ipv4","action":"info","data":{"ip":"::1"}}')), ensure_ascii=False, indent=2)[:600])


if __name__ == "__main__":
    main()
