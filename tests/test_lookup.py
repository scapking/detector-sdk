"""Lookup behaviour: merged fields, multi-source cross-check, schema stability."""

from __future__ import annotations

import asyncio
import json

import pytest

from detector import Detector, InvalidIPError, info, parse_ip
from detector.models import DEFAULT_LOCALES, SCHEMA_VERSION

from .conftest import V4_CHINA, V4_CLOUDFLARE, V4_GOOGLE, V6_GOOGLE


def test_ipv4_core_fields(detector: Detector) -> None:
    info = detector.lookup(V4_GOOGLE)
    assert info.found is True
    assert info.ip == V4_GOOGLE
    assert info.version == 4
    assert info.country.iso_code == "US"
    assert info.country.name() == "United States"
    assert info.city.name() == "Mountain View"
    assert info.subdivisions[0].name() == "California"
    assert info.continent.code == "NA"
    assert info.asn.number == 15169
    assert "Google" in info.asn.organization
    assert info.asn.asn == "AS15169"
    assert info.coordinates == pytest.approx((37.422, -122.085))
    assert info.display == "Mountain View, California, United States"


def test_ipv6_core_fields(detector: Detector) -> None:
    info = detector.lookup(V6_GOOGLE)
    assert info.version == 6
    assert info.found is True
    assert info.country.iso_code == "CA"
    assert info.asn.number == 15169
    assert info.network.endswith("/42") or info.network.endswith("/32")


def test_every_applicable_database_contributes(detector: Detector) -> None:
    """Every loaded file that can answer for IPv4 shows up - and nothing else."""
    expected = {
        db.info.uid for db in detector.databases if 4 in db.ip_versions
    }
    info = detector.lookup(V4_GOOGLE)
    assert set(info.sources) == expected
    assert set(info.raw) == expected
    assert set(info.networks) == expected
    # The IPv6-only GeoLite2 file must not answer for an IPv4 address.
    assert "geolite2-city-ipv6" not in expected
    assert "geolite2-city-ipv4" in expected


def test_address_family_gating(detector: Detector) -> None:
    """v4-only / v6-only files never see the other family - and never raise."""
    v4 = detector.lookup(V4_GOOGLE)
    v6 = detector.lookup(V6_GOOGLE)
    assert "geolite2-city-ipv4" in v4.raw
    assert "geolite2-city-ipv6" not in v4.raw
    assert "geolite2-city-ipv6" in v6.raw
    assert "geolite2-city-ipv4" not in v6.raw
    assert v6.country.iso_code == "CA"


def test_flat_schema_is_mapped(detector: Detector) -> None:
    """ip-location-db's GeoLite2 builds use a flat schema (state1/state2/...)."""
    raw = detector.lookup(V4_GOOGLE).raw["geolite2-city-ipv4"]
    assert {"country_code", "latitude", "longitude"} <= set(raw)
    info = detector.lookup(V4_CHINA)
    flat = info.raw["geolite2-city-ipv4"]
    if flat.get("state2"):
        assert info.subdivisions  # state2 -> subdivision
    if flat.get("postcode"):
        assert info.postal == flat["postcode"] or info.postal


def test_cross_check_and_agreement(detector: Detector) -> None:
    info = detector.lookup(V4_GOOGLE)
    country_sources = info.cross_check["country"]
    assert len(country_sources) >= 4
    assert set(country_sources.values()) == {"US"}
    assert info.agreement["country"] is True
    assert info.agreement["asn"] is True
    # Different products spell the operator differently - that is a real conflict.
    organizations = set(info.cross_check["asn_organization"].values())
    assert len(organizations) > 1
    assert "asn_organization" in info.conflicts


def test_china_ip_is_chinese(detector: Detector) -> None:
    info = detector.lookup(V4_CHINA)
    assert info.country.iso_code == "CN"
    assert info.country.name() == "China"
    assert info.city.name() == "Jinan"
    assert info.continent.code == "AS"
    assert info.asn.number == 137702


def test_private_and_reserved_ips(detector: Detector) -> None:
    info = detector.lookup("192.168.1.1")
    assert info.found is False
    assert info.is_private is True
    assert info.is_global is False
    assert info.country is None

    loopback = detector.lookup("127.0.0.1")
    assert loopback.is_loopback is True

    assert detector.lookup("239.1.1.1").is_multicast is True
    assert detector.lookup("::").is_unspecified is True


def test_invalid_input(detector: Detector) -> None:
    with pytest.raises(InvalidIPError) as excinfo:
        detector.lookup("not-an-ip")
    assert excinfo.value.code == "invalid_ip"
    assert excinfo.value.to_dict()["code"] == "invalid_ip"

    with pytest.raises(InvalidIPError):
        detector.lookup("")

    with pytest.raises(InvalidIPError):
        detector.lookup("999.1.1.1")

    # batch mode never raises: it reports the problem per row
    results = detector.lookup_many([V4_GOOGLE, "nope"])
    assert results[0].found is True
    assert results[1].found is False
    assert results[1].meta["error"]["code"] == "invalid_ip"


def test_batch_preserves_order(detector: Detector) -> None:
    inputs = [V4_GOOGLE, V4_CLOUDFLARE, V4_CHINA, V6_GOOGLE]
    results = detector.lookup_many(inputs)
    assert [item.ip for item in results] == inputs


def test_stream_is_lazy(detector: Detector) -> None:
    seen = []

    def source():
        for ip in (V4_GOOGLE, V4_CLOUDFLARE, "bad-ip"):
            seen.append(ip)
            yield ip

    iterator = detector.stream(source())
    assert seen == []  # nothing consumed yet
    first = next(iterator)
    assert first.ip == V4_GOOGLE
    assert seen == [V4_GOOGLE]
    rest = list(iterator)
    assert [item.found for item in rest] == [True, False]


@pytest.mark.parametrize(
    "value,expected",
    [
        ("8.8.8.8", "8.8.8.8"),
        (" 8.8.8.8 ", "8.8.8.8"),
        ("8.8.8.0/24", "8.8.8.0"),
        ("8.8.8.8:443", "8.8.8.8"),
        ("[2001:4860:4860::8888]:443", "2001:4860:4860::8888"),
        (134744072, "8.8.8.8"),
    ],
)
def test_parse_ip_variants(value: object, expected: str) -> None:
    assert str(parse_ip(value)) == expected


def test_schema_is_stable_and_json_ready(detector: Detector) -> None:
    info = detector.lookup(V4_GOOGLE)
    document = info.to_dict()
    expected = {
        "ip", "version", "found", "network", "networks", "flags", "continent", "country",
        "subdivisions", "city", "location", "postal", "time_zone", "asn", "traits",
        "hostname", "display", "sources", "cross_check", "agreement", "conflicts", "raw",
        "meta",
    }
    assert set(document) == expected
    assert document["meta"]["schema_version"] == SCHEMA_VERSION
    assert document["meta"]["locales"] == list(DEFAULT_LOCALES)
    assert document["meta"]["attribution"]
    parsed = json.loads(info.to_json())
    assert parsed["ip"] == V4_GOOGLE
    assert set(parsed) == expected


def test_flat_dict_and_get(detector: Detector) -> None:
    info = detector.lookup(V4_GOOGLE)
    assert info.get("country.iso_code") == "US"
    assert info.get("city.name") == "Mountain View"
    assert info.get("missing.field", "fallback") == "fallback"
    flat = info.to_flat_dict()
    assert flat["country.iso_code"] == "US"
    assert flat["location.latitude"] == pytest.approx(37.422)
    assert flat["flags.is_private"] is False


def test_options_trim_output(detector: Detector) -> None:
    lean = Detector(include_raw=False, include_cross_check=False, cache_size=0)
    try:
        info = lean.lookup(V4_GOOGLE)
        assert info.raw == {}
        assert info.cross_check == {}
        assert info.country.iso_code == "US"  # merged value is still there
    finally:
        lean.close()


def test_english_only_mode(detector: Detector) -> None:
    strict = Detector(include_all_names=False, locales=("en",), cache_size=0)
    try:
        document = strict.lookup(V4_CHINA).to_dict()
        assert document["country"]["names"] == {"en": "China"}
        assert strict.lookup(V4_CHINA).to_json().isascii()
    finally:
        strict.close()

    # default keeps every language the source ships (that is "all the data")
    wide = detector.lookup(V4_CHINA).to_dict()
    assert len(wide["country"]["names"]) > 5


def test_dataset_filtering() -> None:
    only_city = Detector(datasets=["dbip-city"], cache_size=0)
    try:
        assert only_city.dataset_keys == ["dbip-city"]
        info = only_city.lookup(V4_GOOGLE)
        assert info.country.iso_code == "US"
        assert info.asn is None  # no ASN database loaded
    finally:
        only_city.close()

    without = Detector(exclude=["user-country", "server-country"], cache_size=0)
    try:
        assert "user-country" not in without.dataset_keys
        assert "dbip-city" in without.dataset_keys
    finally:
        without.close()


def test_public_default_is_standard_json() -> None:
    """`await info(ip)` returns the JSON document, not a model object."""
    document = asyncio.run(info(V4_GOOGLE))
    assert isinstance(document, dict)
    assert document["ip"] == V4_GOOGLE
    assert document["country"]["iso_code"] == "US"
    assert set(document) >= {"ip", "country", "asn", "cross_check", "raw", "meta"}

    batch = asyncio.run(info([V4_GOOGLE, V4_CLOUDFLARE]))
    assert isinstance(batch, list) and all(isinstance(row, dict) for row in batch)
    assert [row["ip"] for row in batch] == [V4_GOOGLE, V4_CLOUDFLARE]

    model = asyncio.run(info(V4_GOOGLE, as_object=True))
    assert type(model).__name__ == "IPInfo"
    assert model.country.iso_code == "US"


def test_describe_and_stats(detector: Detector) -> None:
    described = detector.describe()
    assert described["schema_version"] == SCHEMA_VERSION
    assert len(described["databases"]) == len(detector.databases)
    for entry in described["databases"]:
        assert entry["name"] and entry["license"] and entry["build_date"]
    assert "DB-IP" in described["attribution"]
    assert described["bundled_datasets"]
    stats = detector.stats()
    assert stats["databases"] >= 8
    assert set(stats["kinds"]) >= {"city", "asn", "country"}


def test_thread_safety(detector: Detector) -> None:
    import threading

    results = []
    lock = threading.Lock()

    def worker(ip: str) -> None:
        info = detector.lookup(ip)
        with lock:
            results.append((info.ip, info.country.iso_code))

    threads = [threading.Thread(target=worker, args=(V4_GOOGLE,)) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(results) == 16
    assert set(results) == {(V4_GOOGLE, "US")}


def test_context_manager() -> None:
    with Detector(cache_size=0) as instance:
        assert instance.lookup(V4_GOOGLE).found is True
