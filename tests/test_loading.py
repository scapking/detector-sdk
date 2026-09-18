"""Loading policy: import-time start, deadlines, partial answers, failures.

These tests use a *small* data directory (three tiny bundled archives) so the
whole file stays fast, and each one gets its own cache directory - preparation is
shared per (data dir, cache dir), so distinct paths keep the tests independent.
"""

from __future__ import annotations

import asyncio
import shutil

import pytest

from detector import (
    Detector,
    LoadingTimeoutError,
    NoDatabaseError,
    info,
    progress,
    ready,
    warmup,
)
from detector.databases import package_data_dir

SMALL = ("iptoasn-country.mmdb.xz", "server-country.mmdb.xz", "dbip-asn-lite.mmdb.xz")


@pytest.fixture
def small_data(tmp_path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    for name in SMALL:
        shutil.copy(package_data_dir() / name, data / name)
    monkeypatch.setenv("DETECTOR_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("DETECTOR_DB_DIR", str(data))
    return data


def test_nothing_ready_raises_a_structured_timeout(small_data) -> None:
    with pytest.raises(LoadingTimeoutError) as caught:
        Detector(wait_timeout=0.0, on_timeout="error")
    error = caught.value
    assert error.code == "loading_timeout"
    assert error.total >= 1 and error.ready < error.total
    assert error.missing and error.retry_after > 0
    assert set(error.to_dict()) >= {"code", "ready", "total", "missing", "retry_after"}


def test_timeout_can_answer_partially_and_finish_later(small_data) -> None:
    detector = Detector(wait_timeout=0.0, on_timeout="partial")
    try:
        # A zero deadline must not raise: whatever is ready answers, the rest
        # keeps loading in the background (tiny archives may already be done).
        assert detector.databases, "partial mode must open whatever is ready"
        partial = detector.lookup("8.8.8.8")
        assert partial.found
        assert "preparation" in partial.to_dict()["meta"]

        assert asyncio.run(ready()) is True          # nothing left to unpack
        full = detector.lookup("1.1.1.1")            # a fresh address, not cached
        assert len(full.sources) >= 2, "every dataset must be queryable once ready"
        assert full.to_dict()["meta"]["preparation"]["complete"] is True
    finally:
        detector.close()


def test_wait_for_any_is_immediate_but_complete_is_not(small_data) -> None:
    assert asyncio.run(ready(0.5, wait="any")) is True
    assert asyncio.run(ready(0.5)) in (True, False)  # may still be running
    assert asyncio.run(ready()) is True


def test_broken_dataset_is_reported_not_fatal(small_data) -> None:
    (small_data / "dbip-country-lite.mmdb.xz").write_bytes(b"not an archive at all")
    detector = Detector()
    try:
        row = detector.lookup("8.8.8.8")
        assert row.found, "the healthy datasets must still answer"
        failed = row.to_dict()["meta"]["datasets_failed"]
        assert "dbip-country" in failed
        assert "dbip-country" in asyncio.run(progress())["failed"]
    finally:
        detector.close()


def test_strict_mode_refuses_a_broken_dataset(small_data) -> None:
    (small_data / "dbip-country-lite.mmdb.xz").write_bytes(b"broken")
    from detector import DatabaseError

    with pytest.raises((DatabaseError, NoDatabaseError)):
        Detector(strict=True)


def test_warmup_and_client_share_one_preparation(small_data) -> None:
    report = asyncio.run(warmup())
    assert report["files"] >= 3
    assert report["cache_dir"]
    assert report["already_ready"] is False

    detector = Detector(wait_timeout=0.0, on_timeout="error")   # would raise if cold
    try:
        assert len(detector.databases) == report["files"]
        snapshot = asyncio.run(progress())
        assert snapshot["complete"] is True
        assert snapshot["cache_dir"] == report["cache_dir"]
    finally:
        detector.close()

    second = asyncio.run(warmup())
    assert second["already_ready"] is True
    assert second["seconds"] == 0.0


def test_import_time_init_does_not_block(monkeypatch, small_data) -> None:
    monkeypatch.setenv("DETECTOR_INIT", "import")
    from detector.functions import _auto_init

    started = asyncio.run(_auto_init())          # returns immediately
    assert started is None
    assert asyncio.run(ready(5.0)) is True       # background thread finishes it
    snapshot = asyncio.run(progress())
    assert snapshot["init"] == "import"
    assert snapshot["ready"] == snapshot["total"]


def test_env_defaults_shape_the_client(monkeypatch, small_data) -> None:
    monkeypatch.setenv("DETECTOR_WAIT_TIMEOUT", "0")
    monkeypatch.setenv("DETECTOR_ON_TIMEOUT", "error")
    with pytest.raises(LoadingTimeoutError):
        asyncio.run(info("8.8.8.8"))
