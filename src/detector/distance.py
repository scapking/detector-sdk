"""Distance computation.

"Keep it simple" is a design choice here: the default is the haversine
great-circle distance - O(1), no dependencies, roughly one microsecond per call.
Pass ``method="vincenty"`` when you want WGS84 ellipsoid accuracy (millimetre
level, ~30 microseconds per call).

IP geolocation itself has tens to thousands of kilometres of error, so paying
30x CPU for a 0.3% difference is almost never worth it. Batch comparisons of
30 million pairs will care; single lookups will not.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from .models import IPInfo, JsonModel, pick_name

__all__ = [
    "EARTH_RADIUS_KM",
    "KM_PER_MILE",
    "haversine_km",
    "vincenty_km",
    "METHODS",
    "get_method",
    "Distance",
    "distance_between",
]

#: IUGG mean Earth radius
EARTH_RADIUS_KM = 6371.0088
KM_PER_MILE = 1.609344

# WGS84 ellipsoid parameters (for vincenty)
_WGS84_A = 6378137.0
_WGS84_F = 1 / 298.257223563
_WGS84_B = _WGS84_A * (1 - _WGS84_F)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


def vincenty_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Vincenty geodesic distance on the WGS84 ellipsoid, in kilometres.

    Raises ``ValueError`` for nearly antipodal points (no convergence).
    """
    if lat1 == lat2 and lon1 == lon2:
        return 0.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    big_l = math.radians(lon2 - lon1)
    u1 = math.atan((1 - _WGS84_F) * math.tan(phi1))
    u2 = math.atan((1 - _WGS84_F) * math.tan(phi2))
    sin_u1, cos_u1 = math.sin(u1), math.cos(u1)
    sin_u2, cos_u2 = math.sin(u2), math.cos(u2)

    lam = big_l
    for _ in range(200):
        sin_lam, cos_lam = math.sin(lam), math.cos(lam)
        sin_sigma = math.sqrt(
            (cos_u2 * sin_lam) ** 2 + (cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lam) ** 2
        )
        if sin_sigma == 0.0:
            return 0.0
        cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos_lam
        sigma = math.atan2(sin_sigma, cos_sigma)
        sin_alpha = cos_u1 * cos_u2 * sin_lam / sin_sigma
        cos_sq_alpha = 1.0 - sin_alpha * sin_alpha
        if cos_sq_alpha == 0.0:  # equatorial line
            cos_2sigma_m = 0.0
        else:
            cos_2sigma_m = cos_sigma - 2.0 * sin_u1 * sin_u2 / cos_sq_alpha
        c = _WGS84_F / 16.0 * cos_sq_alpha * (4.0 + _WGS84_F * (4.0 - 3.0 * cos_sq_alpha))
        previous = lam
        lam = big_l + (1.0 - c) * _WGS84_F * sin_alpha * (
            sigma + c * sin_sigma * (cos_2sigma_m + c * cos_sigma * (-1.0 + 2.0 * cos_2sigma_m ** 2))
        )
        if abs(lam - previous) < 1e-12:
            break
    else:  # pragma: no cover - antipodal edge case
        raise ValueError("vincenty did not converge")

    u_sq = cos_sq_alpha * (_WGS84_A ** 2 - _WGS84_B ** 2) / _WGS84_B ** 2
    big_a = 1.0 + u_sq / 16384.0 * (4096.0 + u_sq * (-768.0 + u_sq * (320.0 - 175.0 * u_sq)))
    big_b = u_sq / 1024.0 * (256.0 + u_sq * (-128.0 + u_sq * (74.0 - 47.0 * u_sq)))
    delta_sigma = big_b * sin_sigma * (
        cos_2sigma_m
        + big_b / 4.0 * (
            cos_sigma * (-1.0 + 2.0 * cos_2sigma_m ** 2)
            - big_b / 6.0 * cos_2sigma_m * (-3.0 + 4.0 * sin_sigma ** 2)
            * (-3.0 + 4.0 * cos_2sigma_m ** 2)
        )
    )
    return _WGS84_A * big_a * (sigma - delta_sigma) / 1000.0


METHODS: Dict[str, Callable[[float, float, float, float], float]] = {
    "haversine": haversine_km,
    "great-circle": haversine_km,
    "spherical": haversine_km,
    "vincenty": vincenty_km,
    "geodesic": vincenty_km,
    "ellipsoid": vincenty_km,
}


def get_method(name: str) -> Callable[[float, float, float, float], float]:
    try:
        return METHODS[name.strip().lower()]
    except KeyError:
        raise ValueError(
            f"unknown distance method {name!r}; available: {sorted(set(METHODS))}"
        ) from None


@dataclass
class Distance(JsonModel):
    """Distance between two IP addresses."""

    source: str
    target: str
    km: Optional[float] = None
    mi: Optional[float] = None
    method: str = "haversine"

    same_country: Optional[bool] = None
    same_city: Optional[bool] = None
    same_continent: Optional[bool] = None
    same_asn: Optional[bool] = None

    source_location: Optional[Dict[str, Any]] = None
    target_location: Optional[Dict[str, Any]] = None

    #: Why ``km`` is None: no_location / invalid_ip / compute_failed
    reason: Optional[str] = None

    @property
    def available(self) -> bool:
        return self.km is not None

    @property
    def km_int(self) -> Optional[int]:
        return None if self.km is None else round(self.km)

    def __float__(self) -> float:
        return float(self.km) if self.km is not None else float("nan")

    def __str__(self) -> str:
        if self.km is None:
            return f"{self.source} -> {self.target}: unavailable ({self.reason})"
        return f"{self.source} -> {self.target}: {self.km:,.2f} km"

    def to_dict(self, *, locales: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "distance_km": self.km,
            "distance_mi": self.mi,
            "method": self.method,
            "same_country": self.same_country,
            "same_city": self.same_city,
            "same_continent": self.same_continent,
            "same_asn": self.same_asn,
            "source_location": self.source_location,
            "target_location": self.target_location,
            "reason": self.reason,
        }

    @classmethod
    def between(
        cls,
        source: "IPInfo | str",
        target: "IPInfo | str",
        *,
        method: str = "haversine",
        round_ndigits: int = 2,
    ) -> "Distance":
        """Compute from two ``IPInfo`` objects (or two raw IP strings)."""
        return distance_between(source, target, method=method, round_ndigits=round_ndigits)


def _cmp(left: Any, right: Any) -> Optional[bool]:
    if left is None or right is None:
        return None
    return left == right


def _same_city(a: IPInfo, b: IPInfo) -> Optional[bool]:
    if not a.city or not b.city:
        return None
    if a.city.geoname_id is not None and b.city.geoname_id is not None:
        return a.city.geoname_id == b.city.geoname_id
    left = pick_name(a.city.names, ("en",)) or ""
    right = pick_name(b.city.names, ("en",)) or ""
    if not left or not right:
        return None
    return left == right


def distance_between(
    source: "IPInfo | str",
    target: "IPInfo | str",
    *,
    method: str = "haversine",
    round_ndigits: int = 2,
) -> Distance:
    """Distance between two ``IPInfo`` objects or two raw IP strings.

    Raw strings carry no coordinates, so the result reports ``reason="no_location"``
    unless ``IPInfo`` objects are supplied.
    """
    func = get_method(method)

    def ip_of(value: Any) -> str:
        return value.ip if isinstance(value, IPInfo) else str(value)

    def point_of(value: Any) -> Optional[Tuple[float, float]]:
        return value.coordinates if isinstance(value, IPInfo) else None

    result = Distance(source=ip_of(source), target=ip_of(target), method=method)
    left = point_of(source)
    right = point_of(target)

    if isinstance(source, IPInfo):
        result.source_location = source.location.to_dict() if source.location else None
    if isinstance(target, IPInfo):
        result.target_location = target.location.to_dict() if target.location else None

    if left is None or right is None:
        result.reason = "no_location"
        return result

    try:
        km = func(left[0], left[1], right[0], right[1])
    except ValueError as exc:
        result.reason = f"compute_failed: {exc}"
        return result

    result.km = round(km, round_ndigits)
    result.mi = round(km / KM_PER_MILE, round_ndigits)

    if isinstance(source, IPInfo) and isinstance(target, IPInfo):
        result.same_country = _cmp(source.country_code, target.country_code)
        result.same_continent = _cmp(source.continent_code, target.continent_code)
        result.same_city = _same_city(source, target)
        result.same_asn = _cmp(source.asn_number, target.asn_number)
    return result
