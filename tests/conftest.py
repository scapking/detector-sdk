"""Shared fixtures.

One :class:`~detector.Detector` per session: opening eight datasets costs a few
hundred milliseconds, and every test only reads through it.
"""

from __future__ import annotations

import pytest

from detector import Detector

V4_GOOGLE = "8.8.8.8"
V4_CLOUDFLARE = "1.1.1.1"
V4_CHINA = "114.114.114.114"
V4_ALIBABA = "223.5.5.5"
V6_GOOGLE = "2001:4860:4860::8888"
V6_ALIBABA = "2400:3200::1"


@pytest.fixture(scope="session")
def detector() -> Detector:
    instance = Detector(cache_size=0)
    yield instance
    instance.close()


@pytest.fixture(scope="session")
def ips() -> list:
    return [V4_GOOGLE, V4_CLOUDFLARE, V4_CHINA, V4_ALIBABA, V6_GOOGLE, V6_ALIBABA]
