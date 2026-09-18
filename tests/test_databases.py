"""Database layer units: registry, normalization, extraction, manifest."""

from __future__ import annotations

import json
import lzma
import shutil
from pathlib import Path

import pytest

from detector import Detector, known_datasets, read_manifest, write_manifest
from detector.databases import (
    BUILTIN_DATASETS,
    BUNDLED_KEYS,
    NON_REDISTRIBUTABLE_KEYS,
    OPTIONAL_KEYS,
    _part_siblings,
    detect_kind,
    ensure_extracted,
    extraction_dir,
    load_manifest,
    member_for_filename,
    normalize_record,
    package_data_dir,
    part_info,
    spec_for_filename,
    user_cache_dir,
)


def test_registry_covers_the_upstream_project() -> None:
    expected = {
        "dbip-city", "dbip-asn", "dbip-country",
        "iptoasn-asn", "iptoasn-country", "origin-asn",
        "user-country", "server-country",
        "geolite2-city", "geolite2-asn", "geolite2-country",
    }
    assert expected == set(BUILTIN_DATASETS)
    # Everything the upstream project publishes ships in the package.
    assert set(BUNDLED_KEYS) == expected
    assert OPTIONAL_KEYS == ()
    assert set(NON_REDISTRIBUTABLE_KEYS) == {"geolite2-city", "geolite2-asn", "geolite2-country"}
    for spec in BUILTIN_DATASETS.values():
        assert spec.license
        assert spec.attribution
        assert spec.members, spec.key
        assert spec.providers or spec.kind
        for member in spec.members:
            assert member.sources, f"{spec.key}/{member.variant}"
            url = member.sources[0].url_template.format(key=spec.key, period="2026-09")
            assert url.startswith("http")


def test_known_datasets_shape() -> None:
    registry = known_datasets()
    assert registry["dbip-city"]["bundled"] is True
    assert registry["geolite2-city"]["bundled"] is True          # bundled, but...
    assert registry["geolite2-city"]["redistributable"] is False  # ...licence-restricted
    assert registry["dbip-city"]["files"] == ["dbip-city-lite.mmdb.xz"]
    assert len(registry["geolite2-city"]["files"]) == 2
    assert "PDDL" in registry["user-country"]["license"]
    assert "CC BY 4.0" in registry["dbip-asn"]["license"]


def test_bundled_files_exist_and_match_registry() -> None:
    data_dir = package_data_dir()
    for key in BUNDLED_KEYS:
        spec = BUILTIN_DATASETS[key]
        for member in spec.members:
            path = data_dir / member.filename
            parts = _part_siblings(data_dir, member)
            assert path.is_file() or parts, f"missing bundled dataset: {member.filename}"
            surfaces = [path] if path.is_file() else parts
            assert sum(item.stat().st_size for item in surfaces) > 1024
            for surface in surfaces:
                matched_spec, matched_member = member_for_filename(surface.name)
                assert matched_spec is not None and matched_spec.key == key
                assert matched_member is not None and matched_member.filename == member.filename
                assert spec_for_filename(surface.name).key == key


def test_bundled_data_is_split_for_parallel_first_run() -> None:
    """The big databases ship as parts, so first-run decompression parallelises."""
    data_dir = package_data_dir()
    spec = BUILTIN_DATASETS["dbip-city"]
    parts = _part_siblings(data_dir, spec.members[0])
    assert len(parts) >= 2, "the largest database should be split into parts"
    assert all(part_info(item.name) is not None for item in parts)
    assert [part_info(item.name)[1] for item in parts] == sorted(
        part_info(item.name)[1] for item in parts
    )


def test_build_dates_come_from_the_database_metadata() -> None:
    detector = Detector(datasets=["dbip-city"], cache_size=0)
    try:
        info = detector.databases[0].info
        assert info.build_date and info.build_date.startswith("202")
        assert info.license == "CC BY 4.0"
        assert info.providers
    finally:
        detector.close()


def test_detect_kind() -> None:
    assert detect_kind("DBIP-City-Lite") == "city"
    assert detect_kind("GeoLite2-Country") == "country"
    assert detect_kind("DBIP-ASN-Lite (compat=GeoLite2-ASN)") == "asn"
    assert detect_kind("GeoIP2-Enterprise") == "enterprise"
    assert detect_kind("asn ipvAll") == "asn"
    assert detect_kind("country ipvAll") == "country"
    assert detect_kind("something-else") == "unknown"


def test_normalize_maxmind_style_record() -> None:
    record = {
        "city": {"names": {"en": "Mountain View"}},
        "continent": {"code": "NA", "geoname_id": 6255149, "names": {"en": "North America"}},
        "country": {"iso_code": "US", "geoname_id": 6252001, "names": {"en": "United States"},
                    "is_in_european_union": False},
        "location": {"latitude": 37.422, "longitude": -122.085, "accuracy_radius": 1000},
        "postal": {"code": "94043"},
        "subdivisions": [{"names": {"en": "California"}, "iso_code": "CA"}],
        "traits": {"connection_type": "hosting", "is_anycast": True},
    }
    parts = normalize_record(record)
    assert parts.country.iso_code == "US"
    assert parts.country.is_eu is False
    assert parts.city.name() == "Mountain View"
    assert parts.subdivisions[0].iso_code == "CA"
    assert parts.postal == "94043"
    assert parts.location.accuracy_radius == 1000
    assert parts.location.time_zone is None
    assert parts.traits["connection_type"] == "hosting"
    assert parts.traits["is_anycast"] is True
    assert parts.asn is None


def test_normalize_flat_record_variants() -> None:
    flat = normalize_record({"country_code": "US"})
    assert flat.country.iso_code == "US"

    asn = normalize_record({"autonomous_system_number": 15169,
                            "autonomous_system_organization": "GOOGLE"})
    assert asn.asn.number == 15169
    assert asn.asn.organization == "GOOGLE"

    loose = normalize_record(
        {
            "country_name": "Germany",
            "country_code": "DE",
            "latitude": 50.11,
            "longitude": 8.68,
            "asn": 3320,
            "as_name": "Deutsche Telekom AG",
            "region": "Hesse",
            "city_name": "Frankfurt",
            "postal_code": "60313",
        }
    )
    assert loose.country.iso_code == "DE"
    assert loose.country.names["en"] == "Germany"
    assert loose.city.name() == "Frankfurt"
    assert loose.subdivisions[0].name() == "Hesse"
    assert loose.postal == "60313"
    assert loose.asn.number == 3320
    assert loose.location.valid


def test_normalize_keeps_unknown_fields() -> None:
    parts = normalize_record({"country_code": "US", "weird_field": {"a": 1}, "another": 7})
    assert parts.traits["weird_field"] == {"a": 1}
    assert parts.traits["another"] == 7


def test_custom_mmdb_file_is_loaded(tmp_path: Path) -> None:
    """Any MMDB v2.0 file can be plugged in: build one by copying a bundled DB."""
    source = package_data_dir() / BUILTIN_DATASETS["dbip-asn"].filename
    with lzma.open(source, "rb") as handle:
        data = handle.read()
    custom = tmp_path / "my-own-asn.mmdb"
    custom.write_bytes(data)

    detector = Detector(
        databases={"my-own-asn": custom},
        cache_size=0,
        include_cross_check=False,
    )
    try:
        assert detector.dataset_keys == ["my-own-asn"]
        info = detector.lookup("8.8.8.8")
        assert info.asn.number == 15169
        assert info.sources["my-own-asn"]
        assert info.raw["my-own-asn"]["autonomous_system_number"] == 15169
    finally:
        detector.close()


def test_database_directory_discovery(tmp_path: Path) -> None:
    """Files dropped into a directory are picked up, and the registry names them."""
    source = package_data_dir() / BUILTIN_DATASETS["dbip-asn"].filename
    shutil.copy(source, tmp_path / source.name)
    detector = Detector(db_dir=tmp_path, cache_size=0)
    try:
        assert detector.dataset_keys == ["dbip-asn"]
        assert detector.databases[0].info.name == "DBIP-ASN-Lite"
    finally:
        detector.close()


def test_ensure_extracted_is_idempotent(tmp_path: Path) -> None:
    source = package_data_dir() / BUILTIN_DATASETS["dbip-asn"].filename
    assert source.name.endswith(".mmdb.xz")       # shipped format
    first = ensure_extracted(source, tmp_path)
    assert first.is_file() and first.name.endswith(".mmdb")
    assert first.parent == extraction_dir(tmp_path)
    stamp = first.stat().st_mtime_ns
    second = ensure_extracted(source, tmp_path)
    assert second == first
    assert second.stat().st_mtime_ns == stamp


def test_multi_file_dataset_shares_one_key(detector: Detector) -> None:
    """GeoLite2-City is two files but one dataset key."""
    uids = detector.database_uids
    assert "geolite2-city-ipv4" in uids and "geolite2-city-ipv6" in uids
    assert detector.dataset_keys.count("geolite2-city") == 1
    assert "geolite2-city" in detector.dataset_keys


def test_manifest_round_trip(tmp_path: Path) -> None:
    """Works whether a member ships whole or split into parts."""
    data_dir = package_data_dir()
    for key in ("dbip-asn", "geolite2-city"):
        for member in BUILTIN_DATASETS[key].members:
            surfaces = _part_siblings(data_dir, member)
            if not surfaces:
                surfaces = [data_dir / member.filename]
            for surface in surfaces:
                shutil.copy(surface, tmp_path / surface.name)
    path = write_manifest(tmp_path, ["dbip-asn", "geolite2-city"])
    payload = json.loads(path.read_text())
    keys = {(entry["key"], entry["variant"]) for entry in payload["datasets"]}
    assert keys == {("dbip-asn", ""), ("geolite2-city", "ipv4"), ("geolite2-city", "ipv6")}
    assert all(entry["sha256"] for entry in payload["datasets"])
    assert payload["attribution"]
    split = [entry for entry in payload["datasets"] if entry.get("parts")]
    assert split, "a split member must record its parts"
    assert all(len(entry["part_sizes"]) == len(entry["parts"]) for entry in split)
    assert all(entry["sha256"] for entry in split)
    # Build dates come from MMDB metadata, not the file mtime.
    assert all(entry["build_date"].startswith("202") for entry in payload["datasets"])
    assert read_manifest(tmp_path)["datasets"][0]["key"] == "dbip-asn"
    assert set(load_manifest(tmp_path)) == {"dbip-asn", "geolite2-city:ipv4", "geolite2-city:ipv6"}


def test_missing_database_raises_a_clear_error(tmp_path: Path) -> None:
    from detector import NoDatabaseError

    with pytest.raises(NoDatabaseError):
        Detector(db_dir=tmp_path)


def test_user_cache_dir_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DETECTOR_CACHE_DIR", str(tmp_path / "cache"))
    assert user_cache_dir() == tmp_path / "cache"
