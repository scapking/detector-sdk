"""Distance maths: 1-to-1, 1-to-N, N-by-M, ranking, method agreement."""

from __future__ import annotations

import math

import pytest

from detector import Detector, Distance, distance, distance_between, haversine_km, vincenty_km

from .conftest import V4_ALIBABA, V4_CHINA, V4_CLOUDFLARE, V4_GOOGLE, V6_GOOGLE


def test_one_to_one(detector: Detector) -> None:
    result = detector.distance(V4_GOOGLE, V4_CLOUDFLARE)
    assert isinstance(result, Distance)
    assert result.available
    assert result.km == pytest.approx(11920, abs=60)   # Mountain View -> Sydney
    assert result.mi == pytest.approx(result.km / 1.609344, abs=0.05)
    assert result.method == "haversine"
    assert result.same_country is False
    assert result.same_asn is False
    assert result.same_continent is False
    assert result.source_location["latitude"] == pytest.approx(37.422)


def test_same_ip_is_zero(detector: Detector) -> None:
    result = detector.distance(V4_GOOGLE, V4_GOOGLE)
    assert result.km == 0.0
    assert result.same_country is True
    assert result.same_city is True
    assert result.same_asn is True


def test_one_to_n(detector: Detector, ips: list) -> None:
    targets = [item for item in ips if item != V4_GOOGLE]
    results = detector.distance(V4_GOOGLE, targets)
    assert isinstance(results, list)
    assert len(results) == len(targets)
    assert [item.target for item in results] == targets          # order preserved
    assert all(item.available for item in results)
    # measured from Mountain View: Montreal < Jinan < Sydney
    by_target = {item.target: item.km for item in results}
    assert by_target[V6_GOOGLE] < by_target[V4_CHINA] < by_target[V4_CLOUDFLARE]
    assert by_target[V4_CLOUDFLARE] > 11000


def test_one_to_n_accepts_generators(detector: Detector) -> None:
    results = detector.distance(V4_GOOGLE, (ip for ip in [V4_ALIBABA, V6_GOOGLE]))
    assert [item.target for item in results] == [V4_ALIBABA, V6_GOOGLE]


def test_unbounded_targets(detector: Detector) -> None:
    targets = [V4_CLOUDFLARE, V4_CHINA] * 5000          # 10k targets, no cap
    results = detector.distance(V4_GOOGLE, targets)
    assert len(results) == 10000


def test_methods_agree_within_a_percent(detector: Detector) -> None:
    fast = detector.distance(V4_GOOGLE, V4_CHINA, method="haversine")
    exact = detector.distance(V4_GOOGLE, V4_CHINA, method="vincenty")
    assert exact.method == "vincenty"
    assert abs(fast.km - exact.km) / exact.km < 0.01


def test_math_helpers() -> None:
    # Beijing -> Shanghai, ~1067 km
    km = haversine_km(39.9042, 116.4074, 31.2304, 121.4737)
    assert km == pytest.approx(1067, abs=15)
    assert vincenty_km(39.9042, 116.4074, 31.2304, 121.4737) == pytest.approx(km, rel=0.01)
    assert haversine_km(0.0, 0.0, 0.0, 0.0) == 0.0
    # quarter of the equator
    assert haversine_km(0.0, 0.0, 0.0, 90.0) == pytest.approx(math.pi / 2 * 6371.0088, rel=1e-9)


def test_unknown_method_rejected() -> None:
    with pytest.raises(ValueError):
        Detector(distance_method="teleport")


def test_invalid_target_reports_reason(detector: Detector) -> None:
    results = detector.distance(V4_GOOGLE, ["nonsense", V4_CLOUDFLARE])
    assert results[0].km is None
    assert results[0].reason == "invalid_ip"
    assert results[1].available


def test_missing_coordinates_reports_no_location(detector: Detector) -> None:
    results = detector.distance("192.168.1.1", V4_GOOGLE)
    assert results.km is None
    assert results.reason == "no_location"


def test_distance_many_matrix(detector: Detector) -> None:
    sources = [V4_GOOGLE, V4_CHINA]
    targets = [V4_CLOUDFLARE, V4_ALIBABA, V6_GOOGLE]
    results = detector.distance_many(sources, targets)
    assert len(results) == 6
    assert [(item.source, item.target) for item in results] == [
        (source, target) for source in sources for target in targets
    ]


def test_nearest_ranking(detector: Detector) -> None:
    ranked = detector.nearest(V4_CHINA, [V4_ALIBABA, V4_GOOGLE, V4_CLOUDFLARE, V6_GOOGLE], limit=2)
    assert len(ranked) == 2
    assert ranked[0].km <= ranked[1].km
    assert ranked[0].target in (V4_ALIBABA, V4_CLOUDFLARE)

    capped = detector.nearest(V4_CHINA, [V4_ALIBABA, V4_GOOGLE], max_km=10000)
    assert all(item.km <= 10000 for item in capped)


def test_distance_value_helpers(detector: Detector) -> None:
    result = detector.distance(V4_GOOGLE, V4_ALIBABA)
    assert float(result) == pytest.approx(result.km)
    assert result.km_int == round(result.km)
    assert "km" in str(result)
    payload = result.to_dict()
    assert set(payload) == {
        "source", "target", "distance_km", "distance_mi", "method", "same_country",
        "same_city", "same_continent", "same_asn", "source_location", "target_location",
        "reason",
    }


def test_distance_between_prebuilt_infos(detector: Detector) -> None:
    left = detector.lookup(V4_GOOGLE)
    right = detector.lookup(V4_CHINA)
    assert distance_between(left, right).km == detector.distance(V4_GOOGLE, V4_CHINA).km


def test_module_level_distance() -> None:
    results = distance(V4_GOOGLE, [V4_CLOUDFLARE, V4_CHINA])
    assert len(results) == 2
    assert all(item.available for item in results)
