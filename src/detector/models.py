"""Normalized data models.

Design constraints:

* **Fixed schema** - every model serializes to a stable set of keys, missing
  values stay ``None`` (``null`` in JSON).  Downstream code never needs
  defensive ``.get()`` calls, and schema changes are announced through
  ``meta.schema_version``.
* **English only** - all keys and generated values are English.  ``locales``
  defaults to ``("en",)`` so a fresh install returns English names; callers who
  want another locale pass it explicitly.
* **Nothing is dropped** - every field of every source database ends up
  somewhere: mapped fields on the top level, vendor extras in ``traits``, and
  the complete per-database records in ``raw``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

__all__ = [
    "SCHEMA_VERSION",
    "DEFAULT_LOCALES",
    "JsonModel",
    "Place",
    "City",
    "Country",
    "Continent",
    "Subdivision",
    "Location",
    "ASN",
    "IPInfo",
]

SCHEMA_VERSION = "1.0"

#: English first. Pass an explicit sequence to override per call.
DEFAULT_LOCALES: Tuple[str, ...] = ("en",)


def pick_name(names: Optional[Dict[str, str]], locales: Sequence[str]) -> Optional[str]:
    """Pick a name according to ``locales`` priority with language fallback (en-US -> en)."""
    if not names:
        return None
    for locale in locales:
        value = names.get(locale)
        if value:
            return value
    for locale in locales:
        base = locale.split("-")[0].lower()
        for key, value in names.items():
            if key.split("-")[0].lower() == base and value:
                return value
    for value in names.values():
        if value:
            return value
    return None


def out_names(
    names: Optional[Dict[str, str]],
    locales: Sequence[str],
    all_names: Optional[bool],
) -> Dict[str, str]:
    """Serialize the ``names`` map.

    ``all_names`` is ``True`` (default) - every language the source provides.
    Pass ``False`` for English-only documents: only the requested ``locales``
    survive, which is what callers wanting a strict English payload should use.
    """
    if not names:
        return {}
    if all_names is not False:
        return dict(names)
    kept: Dict[str, str] = {}
    for locale in locales:
        value = names.get(locale)
        if value:
            kept[locale] = value
    return kept


class JsonModel:
    """Base class: uniform ``to_dict`` / ``to_json`` behaviour."""

    def to_dict(
        self,
        *,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> Dict[str, Any]:
        raise NotImplementedError

    def to_json(
        self,
        *,
        indent: Optional[int] = None,
        ensure_ascii: bool = True,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> str:
        return json.dumps(
            self.to_dict(locales=locales, all_names=all_names),
            ensure_ascii=ensure_ascii,
            indent=indent,
            default=str,
        )

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]


@dataclass
class Place(JsonModel):
    """A named geographic entity (city / country / continent / subdivision)."""

    names: Dict[str, str] = field(default_factory=dict)
    geoname_id: Optional[int] = None

    def name(self, locales: Sequence[str] = DEFAULT_LOCALES) -> Optional[str]:
        return pick_name(self.names, locales)

    @property
    def found(self) -> bool:
        return bool(self.names) or self.geoname_id is not None

    def to_dict(
        self,
        *,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> Dict[str, Any]:
        locales = tuple(locales or DEFAULT_LOCALES)
        return {
            "name": pick_name(self.names, locales),
            "names": out_names(self.names, locales, all_names),
            "geoname_id": self.geoname_id,
        }


class City(Place):
    """City."""


@dataclass
class Continent(Place):
    """Continent."""

    code: Optional[str] = None

    def to_dict(
        self,
        *,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> Dict[str, Any]:
        data = super().to_dict(locales=locales, all_names=all_names)
        data["code"] = self.code
        return data


@dataclass
class Country(Place):
    """Country."""

    iso_code: Optional[str] = None
    is_in_eu: Optional[bool] = None

    @property
    def code(self) -> Optional[str]:
        return self.iso_code

    @property
    def is_eu(self) -> Optional[bool]:
        """Alias for :attr:`is_in_eu`."""
        return self.is_in_eu

    def to_dict(
        self,
        *,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> Dict[str, Any]:
        data = super().to_dict(locales=locales, all_names=all_names)
        data["iso_code"] = self.iso_code
        data["is_in_eu"] = self.is_in_eu
        return data


@dataclass
class Subdivision(Place):
    """First-level administrative division (state / province)."""

    iso_code: Optional[str] = None

    def to_dict(
        self,
        *,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> Dict[str, Any]:
        data = super().to_dict(locales=locales, all_names=all_names)
        data["iso_code"] = self.iso_code
        return data


@dataclass
class Location(JsonModel):
    """Coordinates plus accuracy hints."""

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    time_zone: Optional[str] = None
    accuracy_radius: Optional[int] = None

    @property
    def valid(self) -> bool:
        return self.latitude is not None and self.longitude is not None

    @property
    def coordinates(self) -> Optional[Tuple[float, float]]:
        if not self.valid:
            return None
        return (float(self.latitude), float(self.longitude))  # type: ignore[arg-type]

    def to_dict(
        self,
        *,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> Dict[str, Any]:
        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "time_zone": self.time_zone,
            "accuracy_radius": self.accuracy_radius,
        }


@dataclass
class ASN(JsonModel):
    """Autonomous system information."""

    number: Optional[int] = None
    organization: Optional[str] = None

    @property
    def asn(self) -> Optional[str]:
        return f"AS{self.number}" if self.number is not None else None

    @property
    def found(self) -> bool:
        return self.number is not None or bool(self.organization)

    def to_dict(
        self,
        *,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> Dict[str, Any]:
        return {
            "number": self.number,
            "asn": self.asn,
            "organization": self.organization,
        }


@dataclass
class IPInfo(JsonModel):
    """Everything known about one IP address. This *is* the standard output."""

    ip: str
    version: Optional[int] = None
    found: bool = False

    network: Optional[str] = None
    networks: Dict[str, str] = field(default_factory=dict)

    #: Address-class flags, computed with the stdlib (no database needed)
    is_private: Optional[bool] = None
    is_global: Optional[bool] = None
    is_loopback: Optional[bool] = None
    is_reserved: Optional[bool] = None
    is_multicast: Optional[bool] = None
    is_unspecified: Optional[bool] = None

    continent: Optional[Continent] = None
    country: Optional[Country] = None
    subdivisions: List[Subdivision] = field(default_factory=list)
    city: Optional[City] = None
    location: Optional[Location] = None
    postal: Optional[str] = None
    time_zone: Optional[str] = None

    asn: Optional[ASN] = None

    #: Vendor extras that have no home in the fixed schema (connection_type,
    #: is_anycast, domain, user_type, ...). Merged from every database.
    traits: Dict[str, Any] = field(default_factory=dict)

    #: Optional reverse-DNS result (opt-in via ``resolve_dns=True``)
    hostname: Optional[str] = None

    #: Which database answered which field group: {"dbip-city": "DBIP-City-Lite", ...}
    sources: Dict[str, str] = field(default_factory=dict)

    #: Per-source view of every field that several databases can answer:
    #: {"country": {"DBIP-City-Lite": "US", "User-Country": "US"}, ...}
    cross_check: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    #: field -> True when every source that answered the field agrees
    agreement: Dict[str, bool] = field(default_factory=dict)

    #: Field names where sources disagree - worth a look before trusting the value
    conflicts: List[str] = field(default_factory=list)

    #: Full record per source database - nothing is thrown away.
    raw: Dict[str, Any] = field(default_factory=dict)

    #: Database inventory, attribution and schema version.
    meta: Dict[str, Any] = field(default_factory=dict)

    #: Serialization default: True keeps every language of every ``names`` map,
    #: False trims them to the requested locales (strict English payloads).
    _all_names: bool = field(default=True, repr=False, compare=False)

    # ---------------- convenience accessors ----------------

    @property
    def country_code(self) -> Optional[str]:
        return self.country.iso_code if self.country else None

    @property
    def continent_code(self) -> Optional[str]:
        return self.continent.code if self.continent else None

    @property
    def coordinates(self) -> Optional[Tuple[float, float]]:
        return self.location.coordinates if self.location else None

    @property
    def asn_number(self) -> Optional[int]:
        return self.asn.number if self.asn else None

    @property
    def asn_organization(self) -> Optional[str]:
        return self.asn.organization if self.asn else None

    @property
    def city_name(self) -> Optional[str]:
        return self.city.name() if self.city else None

    @property
    def country_name(self) -> Optional[str]:
        return self.country.name() if self.country else None

    @property
    def display(self) -> Optional[str]:
        """``"City, Subdivision, Country"`` in the client's default locale order."""
        return self.localized_name()

    def localized_name(self, locales: Sequence[str] = DEFAULT_LOCALES) -> Optional[str]:
        """Human readable ``"City, Subdivision, Country"`` string."""
        parts: List[str] = []
        for place in (self.city, self.subdivisions[0] if self.subdivisions else None, self.country):
            if place is None:
                continue
            value = place.name(locales)
            if value and value not in parts:
                parts.append(value)
        return ", ".join(parts) if parts else None

    def get(self, path: str, default: Any = None) -> Any:
        """Dotted-path access: ``info.get("country.iso_code")``."""
        node: Any = self.to_dict()
        for part in path.split("."):
            if isinstance(node, dict) and part in node:
                node = node[part]
            elif isinstance(node, list) and part.isdigit() and int(part) < len(node):
                node = node[int(part)]
            else:
                return default
        return node

    def to_flat_dict(self, *, locales: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        """Flatten to ``{"country.iso_code": "US", ...}`` for columnar sinks."""

        def walk(node: Any, prefix: str, out: Dict[str, Any]) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    walk(value, f"{prefix}.{key}" if prefix else key, out)
            elif isinstance(node, list):
                if not node:
                    out[prefix] = []
                for index, value in enumerate(node):
                    walk(value, f"{prefix}.{index}", out)
            else:
                out[prefix] = node

        flat: Dict[str, Any] = {}
        walk(self.to_dict(locales=locales), "", flat)
        return flat

    def to_dict(
        self,
        *,
        locales: Optional[Sequence[str]] = None,
        all_names: Optional[bool] = None,
    ) -> Dict[str, Any]:
        locales = tuple(locales or DEFAULT_LOCALES)
        if all_names is None:
            all_names = self._all_names

        def place(value: Any) -> Optional[Dict[str, Any]]:
            """Serialize a nested place with the same locale/name options."""
            return None if value is None else value.to_dict(locales=locales, all_names=all_names)

        return {
            "ip": self.ip,
            "version": self.version,
            "found": self.found,
            "network": self.network,
            "networks": dict(self.networks),
            "flags": {
                "is_private": self.is_private,
                "is_global": self.is_global,
                "is_loopback": self.is_loopback,
                "is_reserved": self.is_reserved,
                "is_multicast": self.is_multicast,
                "is_unspecified": self.is_unspecified,
            },
            "continent": place(self.continent),
            "country": place(self.country),
            "subdivisions": [place(item) for item in self.subdivisions],
            "city": place(self.city),
            "location": place(self.location),
            "postal": self.postal,
            "time_zone": self.time_zone,
            "asn": place(self.asn),
            "traits": dict(self.traits),
            "hostname": self.hostname,
            "display": self.localized_name(locales),
            "sources": dict(self.sources),
            "cross_check": dict(self.cross_check),
            "agreement": dict(self.agreement),
            "conflicts": list(self.conflicts),
            "raw": dict(self.raw),
            "meta": dict(self.meta),
        }
